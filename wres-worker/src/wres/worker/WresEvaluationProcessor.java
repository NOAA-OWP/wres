package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.http.WebClient;
import wres.http.WebClientUtils;
import wres.messages.generated.Job;
import wres.messages.generated.JobOutput;
import wres.messages.generated.JobResult;


/**
 * Holds a wres evaluation job started with call(), interacts with the worker server also sends messages
 * with stdout and stderr to an exchange via a connection
 */

class WresEvaluationProcessor implements Callable<Integer>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresEvaluationProcessor.class );

    /**
     * Failure code that signified the server is a bad/unrecoverable state, returning this will cycle the worker container
     */
    private static final int META_FAILURE_CODE = 600;

    /** How many seconds we wait before forcing down the executor threads */
    private static final int EXECUTOR_WAIT_TIME = 10;

    private static final String START_EVAL_URI = "http://localhost:%d/evaluation/getEvaluation/%s";

    private static final String OPEN_EVAL_URI = "http://localhost:%d/evaluation/startEvaluation";

    private static final String CLOSE_EVAL_URI = "http://localhost:%d/evaluation/close";

    private static final String CLEAN_DATABASE_URI = "http://localhost:%d/evaluation/cleanDatabase";

    private static final String MIGRATE_DATABASE_URI = "http://localhost:%d/evaluation/migrateDatabase";

    private static final List<Integer> RETRY_STATES = List.of( 503, 504 );

    /** Stream identifier. */
    public enum WhichStream
    {
        /** job output files. */
        OUTPUT,
        /** Exit codes. */
        EXITCODE,

        STDOUT
    }

    private final String exchangeName;
    private final String jobId;
    private final Connection connection;

    private final int port;

    private final byte[] jobMessage;

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient( WebClientUtils.noTimeoutHttpClient() );

    /**
     * The envelope from the message that caused creation of this process,
     * used by the caller to send an ack to the broker.
     */
    private final Envelope envelope;

    WresEvaluationProcessor( String exchangeName,
                             String jobId,
                             Connection connection,
                             Envelope envelope,
                             byte[] jobMessage,
                             int port )
    {
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        this.connection = connection;
        this.envelope = envelope;
        this.jobMessage = jobMessage;
        this.port = port;
    }

    private String getExchangeName()
    {
        return this.exchangeName;
    }

    private String getRoutingKey( WhichStream whichStream )
    {
        return "job." + this.getJobId() + "." + whichStream.name();
    }

    private String getJobId()
    {
        return this.jobId;
    }

    private Connection getConnection()
    {
        return this.connection;
    }

    private Envelope getEnvelope()
    {
        return this.envelope;
    }

    private int getPort()
    {
        return this.port;
    }

    /**
     * Get the delivery tag to send an ack with
     * @return the delivery tag that was used in creating this process.
     */
    long getDeliveryTag()
    {
        return this.getEnvelope().getDeliveryTag();
    }

    /**
     * Execute the process assigned, return the exit code or 600+ if something
     * went wrong talking to the broker.
     * @return exit code of process
     * META_FAILURE_CODE (600) if communication failed.
     */

    public Integer call() throws IOException
    {
        // Convert the job message into a job to see if we need to migrate or clean the database
        Job.job job;
        try
        {
            job = Job.job.parseFrom( this.jobMessage );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            throw new IllegalArgumentException( "Bad message received", ipbe );
        }

        // Check if the Job is to manipulate the database in some way
        if ( job.getVerb().equals( Job.job.Verb.CLEANDATABASE ) )
        {
            return manipulateDatabase( CLEAN_DATABASE_URI );
        }
        if ( job.getVerb().equals( Job.job.Verb.MIGRATEDATABASE ) )
        {
            return manipulateDatabase( MIGRATE_DATABASE_URI );
        }

        // If we do not have a special verb, run this job as an execution
        return executeEvaluation( job );
    }

    /**
     * Method to kick off an evaluations and redirect stdOut and stdErr from server
     * @param job The job we are kicking off
     * @return the status code from the Evaluation
     */
    private int executeEvaluation( Job.job job )
    {
        // Check to see if there is any project config at all
        if ( job.getProjectConfig().isEmpty() )
        {
            UnsupportedOperationException problem =
                    new UnsupportedOperationException( "Could not execute due to invalid message." );
            LOGGER.warn( problem.getMessage() );
            byte[] response =
                    WresEvaluationProcessor.prepareMetaFailureResponse( new UnsupportedOperationException( problem ) );
            this.sendMessage( response, WhichStream.EXITCODE );
            throw problem;
        }

        // Use one Thread per messenger:
        ExecutorService executorService = Executors.newFixedThreadPool( 3 );

        // Open and start an evaluation
        String evaluationId = startEvaluation();

        // Halt evaluation if we are unable to open a project successfully
        // an empty response means there is a bad state on the server but we are accepting a new job
        if ( evaluationId.isEmpty() )
        {
            LOGGER.warn( "Unable to create a new evaluation" );
            byte[] response = WresEvaluationProcessor.prepareExitResponse( HttpURLConnection.HTTP_BAD_REQUEST,
                                                                           "Failed to open eval" );
            this.sendMessage( response, WhichStream.EXITCODE );
            WresEvaluationProcessor.shutdownExecutor( executorService );
            return META_FAILURE_CODE;
        }

        // Set up way to publish the standard output of the process to broker
        // and bind process outputs to message publishers
        JobStandardStreamMessenger stdoutMessenger =
                new JobStandardStreamMessenger( this.getConnection(),
                                                this.getExchangeName(),
                                                this.getJobId(),
                                                JobStandardStreamMessenger.WhichStream.STDOUT,
                                                this.getPort(),
                                                evaluationId );

        JobStandardStreamMessenger stderrMessenger =
                new JobStandardStreamMessenger( this.getConnection(),
                                                this.getExchangeName(),
                                                this.getJobId(),
                                                JobStandardStreamMessenger.WhichStream.STDERR,
                                                this.getPort(),
                                                evaluationId );

        // Send process aliveness messages, like a heartbeat.
        JobStatusMessenger statusMessenger =
                new JobStatusMessenger( this.getConnection(),
                                        this.getExchangeName(),
                                        this.getJobId(),
                                        this.getPort(),
                                        evaluationId );
        executorService.submit( stdoutMessenger );
        executorService.submit( stderrMessenger );
        executorService.submit( statusMessenger );

        try
        {
            // If for some reason a job kicked off does not return ANY response code, ther server is likely in a bad state
            int exitValue = getEvaluationResult( evaluationId );
            String exitMessage = String.format( "Request exited with http code: %s for job: %s", exitValue, jobId );
            LOGGER.info( exitMessage );
            byte[] response = WresEvaluationProcessor.prepareExitResponse( exitValue, null );
            this.sendMessage( response, WhichStream.EXITCODE );
            closeEvaluation();
            WresEvaluationProcessor.shutdownExecutor( executorService );
            return exitValue;
        }
        // There was a meta exception processing an evaluation and the server is in a bad state
        // return meta failure code so the container can be restarted and job dequeued
        catch ( EvaluationProcessingException epe )
        {
            String genericError = """
                    !!!!----------------------------------------------------------------------------------!!!!
                                        
                    This evaluation has failed due to an unrecoverable problem within the WRES.
                    Please do not resubmit your evaluation.
                    Instead, please report this issue by opening a ticket in the WRES User Support project:
                    https://vlab.***REMOVED***/redmine/projects/wres-user-support/issues/new
                                        
                    !!!!----------------------------------------------------------------------------------!!!!
                    """;
            this.sendMessage( prepareStdStreamMessage( genericError ), WhichStream.STDOUT );

            String errorMessage =
                    String.format( "Failed to finish the evaluation for job: %s with log: %n %s", jobId, epe );
            LOGGER.error( errorMessage );
            byte[] response = WresEvaluationProcessor.prepareMetaFailureResponse( epe );
            this.sendMessage( response, WhichStream.EXITCODE );
            WresEvaluationProcessor.shutdownExecutor( executorService );
            return META_FAILURE_CODE;
        }
    }

    /**
     * Helper method to prepare and start an evaluation
     * @return String representation of an evaluation id
     */
    private String startEvaluation()
    {
        URI prepareEval = URI.create( String.format( OPEN_EVAL_URI, this.getPort() ) );
        try ( WebClient.ClientResponse evaluationIdRequest = WEB_CLIENT.postToWeb( prepareEval, jobMessage ) )
        {
            if ( evaluationIdRequest.getStatusCode() == HttpURLConnection.HTTP_BAD_REQUEST )
            {
                // Return empty project ID when we do not get a good response from the server
                return "";
            }
            return new BufferedReader(
                    new InputStreamReader( evaluationIdRequest.getResponse() )
            ).lines().collect( Collectors.joining( "\n" ) );
        }
        catch ( IOException e )
        {
            throw new EvaluationProcessingException( "Unable to prepare the eval ID", e );
        }
    }

    /**
     * Method to kick off and record the output of an EVALUATE call
     * @param evaluationId the ID opened for this job
     * @return and int holding the return value of the call
     */
    private int getEvaluationResult( String evaluationId )
    {
        URI startEvalURI = URI.create( String.format( START_EVAL_URI, this.getPort(), evaluationId ) );
        String startMessage = String.format( "Starting evaluation: %s", startEvalURI );
        LOGGER.info( startMessage );

        try (
                WebClient.ClientResponse clientResponse = WEB_CLIENT.getFromWeb( startEvalURI,
                                                                                 RETRY_STATES )
        )
        {
            LOGGER.info( "Evaluation with internal id {} for job {} has returned", evaluationId, this.getJobId() );
            // The job succeeded and sent output
            if ( clientResponse.getStatusCode() == 200 )
            {
                // Go through the output paths returned from the evaluation post request and send them to the broker
                new BufferedReader(
                        new InputStreamReader( clientResponse.getResponse() ) )
                        .lines()
                        .forEach( out -> this.sendMessage(
                                          prepareOutputPathMessage( Paths.get( out ) ), WhichStream.OUTPUT
                                  )
                        );
            }

            return clientResponse.getStatusCode();
        }
        catch ( IOException e )
        {
            throw new EvaluationProcessingException( "Unable to run an evaluation", e );
        }
    }

    /**
     * Helper method to enable database modification methods to be called like CLEAN and MIGRATE
     * @return String representation of an evaluation id
     */
    private int manipulateDatabase( String uriToCall )
    {
        URI prepareEval = URI.create( String.format( uriToCall, this.getPort() ) );
        try ( WebClient.ClientResponse evaluationIdRequest = WEB_CLIENT.postToWeb( prepareEval, jobMessage ) )
        {
            if ( evaluationIdRequest.getStatusCode() != HttpURLConnection.HTTP_OK )
            {
                LOGGER.error( "Unable to manipulate database" );
            }

            byte[] response = WresEvaluationProcessor.prepareExitResponse( evaluationIdRequest.getStatusCode(), null );
            this.sendMessage( response, WhichStream.EXITCODE );

            LOGGER.info( "Completed request: {}", uriToCall );
            return evaluationIdRequest.getStatusCode();
        }
        catch ( IOException e )
        {
            LOGGER.warn( "Something went wrong manipulating the database. Cycling container to fix this" );
            return META_FAILURE_CODE;
        }
    }

    /**
     * Helper method to close an evaluation to free up the server for the next execution
     */
    private void closeEvaluation()
    {
        try ( WebClient.ClientResponse clientResponse =
                      WEB_CLIENT.postToWeb( URI.create( String.format( CLOSE_EVAL_URI, this.getPort() ) ) ) )
        {
            if ( clientResponse.getStatusCode() != HttpURLConnection.HTTP_OK )
            {
                LOGGER.info( "Evaluation was not able to be closed" );
            }
            LOGGER.info( "Evaluation Closed" );
        }
        catch ( IOException e )
        {
            throw new EvaluationProcessingException( "Unable to close the evaluation", e );
        }
    }

    /**
     * Helper to prepare a completed job response
     * @param exitCode the actual exitCode of the wres process
     * @param detail a message with some details about the job completion, for
     *               example the exception that happened, or null if no message.
     * @return raw message indicating job exit code
     */

    private static byte[] prepareExitResponse( int exitCode, String detail )
    {
        String resolvedDetail = "";

        if ( detail != null )
        {
            resolvedDetail = detail;
        }

        JobResult.job_result jobResult = JobResult.job_result
                .newBuilder()
                .setResult( exitCode )
                .setDetail( resolvedDetail )
                .build();
        return jobResult.toByteArray();
    }

    /**
     * Helper to prepare a job that failed due to never getting the job to run
     * @param e the exception that occurred or null if none
     * @return raw message indicating meta failure
     */

    private static byte[] prepareMetaFailureResponse( Exception e )
    {
        return WresEvaluationProcessor.prepareExitResponse( META_FAILURE_CODE, e.toString() );
    }

    private static byte[] prepareStdStreamMessage( String line )
    {
        LOGGER.info( line );
        wres.messages.generated.JobStandardStream.job_standard_stream message
                = wres.messages.generated.JobStandardStream.job_standard_stream
                .newBuilder()
                .setText( line )
                .build();

        return message.toByteArray();
    }

    /**
     * Attempts to send a message to the broker
     * @param message the message to send.
     */
    private void sendMessage( byte[] message, WhichStream whichStream )
    {
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .deliveryMode( 2 )
                        .build();

        try ( Channel channel = this.getConnection().createChannel() )
        {
            String exchangeType = "topic";
            channel.exchangeDeclare( this.getExchangeName(), exchangeType, true );
            channel.basicPublish( this.getExchangeName(),
                                  this.getRoutingKey( whichStream ),
                                  properties,
                                  message );
        }
        catch ( IOException | TimeoutException ioe )
        {
            LOGGER.warn( "Sending the output to {} failed: {}", message, this.getRoutingKey( whichStream ), ioe );
        }
    }

    /**
     * Prepares the message to send of the output
     * @param path the path of output to send as a message
     * @return the byte array of the output path to send
     */
    private byte[] prepareOutputPathMessage( Path path )
    {
        URI theOutputResource = path.toUri();

        LOGGER.debug( "Sending output uri {} to broker.", theOutputResource );

        JobOutput.job_output jobOutputMessage = JobOutput.job_output
                .newBuilder()
                .setResource( theOutputResource.toString() )
                .build();

        return jobOutputMessage.toByteArray();
    }

    /**
     * Shuts down an executor service, waiting specified time before forcing it.
     * @param executorService the service to shut down
     */

    private static void shutdownExecutor( ExecutorService executorService )
    {
        executorService.shutdown();
        boolean died = false;

        try
        {
            died = executorService.awaitTermination( EXECUTOR_WAIT_TIME, TimeUnit.SECONDS );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while waiting for executor shutdown", ie );
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if ( !died )
        {
            LOGGER.warn( "Executor did not shut down in {} {}, forcing down.",
                         EXECUTOR_WAIT_TIME, TimeUnit.SECONDS );
            executorService.shutdownNow();
        }
    }
}

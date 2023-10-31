package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;

import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.http.WebClient;
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

    private static final int META_FAILURE_CODE = 600;

    private static final String START_EVAL_URI = "http://localhost:%d/evaluation/start/%s";

    private static final String OPEN_EVAL_URI = "http://localhost:%d/evaluation/open";

    private static final String CLOSE_EVAL_URI = "http://localhost:%d/evaluation/close";

    private static final String CLEAN_DATABASE_URI = "http://localhost:%d/evaluation/cleanDatabase";

    private static final String MIGRATE_DATABASE_URI = "http://localhost:%d/evaluation/migrateDatabase";

    private static final List<Integer> RETRY_STATES = List.of( Response.Status.REQUEST_TIMEOUT.getStatusCode() );

    private static final Duration CALL_TIMEOUT = Duration.ofMinutes( 2 );

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
    private static final WebClient WEB_CLIENT = new WebClient();

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

        // Use one Thread per messenger:
        ExecutorService executorService = Executors.newFixedThreadPool( 3 );

        // Open an evaluation for work
        String evaluationId = prepareEvaluationId();

        // Halt evaluation if we are unable to open a project successfully
        if ( evaluationId.isEmpty() )
        {
            LOGGER.warn( "Unable to open a new evaluation" );
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
            int exitValue = META_FAILURE_CODE;
            // Check if the Job is to manipulate the database in some way
            if ( job.getVerb().equals( Job.job.Verb.CLEANDATABASE ) )
            {
                exitValue = manipulateDatabase( CLEAN_DATABASE_URI );
            }
            if ( job.getVerb().equals( Job.job.Verb.MIGRATEDATABASE ) )
            {
                exitValue = manipulateDatabase( MIGRATE_DATABASE_URI );
            }

            // Executes an evaluation
            if ( job.getVerb().equals( Job.job.Verb.EXECUTE ) )
            {
                exitValue = startEvaluation( evaluationId, job );
            }

            LOGGER.info( String.format( "Request exited with http code: %s for job: %s", exitValue, jobId ) );
            byte[] response = WresEvaluationProcessor.prepareExitResponse( exitValue, null );
            this.sendMessage( response, WhichStream.EXITCODE );
            WresEvaluationProcessor.shutdownExecutor( executorService );
            closeEvaluation();
            return exitValue;
        }
        // There was a meta exception processing an evaluation and the server is in a bad state
        // return meta failure code so the container can be restarted and job dequeued
        catch ( EvaluationProcessingException epe )
        {
            String error = "!!!!----------------------------------------------------------------------------------!!!!\n\n"
                    + "This evaluation has failed due to an unrecoverable problem within the WRES.\n"
                    + "Please do not resubmit your evaluation.\n"
                    + "Instead, please report this issue by opening a ticket in the WRES User Support project:\n"
                           + "https://vlab.***REMOVED***/redmine/projects/wres-user-support/issues/new\n\n"
                    +  "!!!!----------------------------------------------------------------------------------!!!!\n";
            this.sendMessage( prepareStdStreamMessage( error ), WhichStream.STDOUT );

            LOGGER.error( String.format( "Failed to finish the evaluation for job: %s with log: \n %s", jobId, epe ) );
            byte[] response = WresEvaluationProcessor.prepareMetaFailureResponse( epe );
            this.sendMessage( response, WhichStream.EXITCODE );
            WresEvaluationProcessor.shutdownExecutor( executorService );
            return META_FAILURE_CODE;
        }
    }

    /**
     * Helper method to prepare an evaluation and create an evaluation ID with the worker server
     * @return String representation of an evaluation id
     * @throws IOException
     */
    private String prepareEvaluationId()
    {
        URI prepareEval = URI.create( String.format( OPEN_EVAL_URI, this.getPort() ) );
        try ( WebClient.ClientResponse evaluationIdRequest = WEB_CLIENT.postToWeb( prepareEval,
                                                                                   CALL_TIMEOUT,
                                                                                   RETRY_STATES ) )
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
     * @param job the job contents
     * @return and int holding the return value of the call
     */
    private int startEvaluation( String evaluationId, Job.job job )
    {
        // Check to see if there is any command at all
        if ( job.getProjectConfig().isEmpty() )
        {
            UnsupportedOperationException problem =
                    new UnsupportedOperationException( "Could not execute due to invalid message." );
            LOGGER.warn( "", problem );
            byte[] response =
                    WresEvaluationProcessor.prepareMetaFailureResponse( new UnsupportedOperationException( problem ) );
            this.sendMessage( response, WhichStream.EXITCODE );
            return META_FAILURE_CODE;
        }

        URI startEvalURI = URI.create( String.format( START_EVAL_URI, this.getPort(), evaluationId ) );
        LOGGER.info( String.format( "Starting evaluation: %s", startEvalURI ) );

        try (
                WebClient.ClientResponse clientResponse = WEB_CLIENT.postToWeb( startEvalURI,
                                                                                jobMessage,
                                                                                CALL_TIMEOUT,
                                                                                RETRY_STATES )
        )
        {
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
        try ( WebClient.ClientResponse evaluationIdRequest = WEB_CLIENT.postToWeb( prepareEval,
                                                                                   jobMessage,
                                                                                   CALL_TIMEOUT,
                                                                                   RETRY_STATES ) )
        {
            if ( evaluationIdRequest.getStatusCode() == HttpURLConnection.HTTP_INTERNAL_ERROR )
            {
                LOGGER.error( "Unable to manipulate database" );
                return META_FAILURE_CODE;
            }

            LOGGER.info( "Database manipulated successfully" );
            return evaluationIdRequest.getStatusCode();
        }
        catch ( IOException e )
        {
            throw new EvaluationProcessingException( "Unable to manipulate the database", e );
        }
    }

    /**
     * Helper method to close an evaluation to free up the server for the next execution
     */
    private void closeEvaluation()
    {
        try ( WebClient.ClientResponse clientResponse =
                      WEB_CLIENT.postToWeb( URI.create( String.format( CLOSE_EVAL_URI, this.getPort() ) ),
                                            CALL_TIMEOUT, RETRY_STATES ) )
        {
            if ( clientResponse.getStatusCode() != HttpsURLConnection.HTTP_OK )
            {
                LOGGER.info( "Evaluation was not able to be closed" );
            }
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

        LOGGER.info( "Sending output uri {} to broker.", theOutputResource );

        JobOutput.job_output jobOutputMessage = JobOutput.job_output
                .newBuilder()
                .setResource( theOutputResource.toString() )
                .build();

        return jobOutputMessage.toByteArray();
    }

    private static void shutdownExecutor( ExecutorService executorService )
    {
        WresEvaluationProcessor.shutdownExecutor( executorService, 10, TimeUnit.SECONDS );
    }

    /**
     * Shuts down an executor service, waiting specified time before forcing it.
     * @param executorService the service to shut down
     * @param wait the quantity of time units to wait before forcible shutdown
     * @param waitUnit the unit of time for wait before forcible shutdown
     */

    private static void shutdownExecutor( ExecutorService executorService,
                                          long wait,
                                          TimeUnit waitUnit )
    {
        executorService.shutdown();
        boolean died = false;

        try
        {
            died = executorService.awaitTermination( wait, waitUnit );
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
                         wait, waitUnit );
            executorService.shutdownNow();
        }
    }
}

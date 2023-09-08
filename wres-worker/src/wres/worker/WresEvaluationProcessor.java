package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.http.WebClient;
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

    private final String exchangeName;
    private final String jobId;
    private final Connection connection;

    private final int port;

    private final String jobMessage;

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
                             String jobMessage,
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

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + ".output";
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
     * @return exit code of process or META_FAILURE_CODE (600+) if broker
     * communication failed.
     */

    public Integer call()
    {
        // Check to see if there is any command at all.
        if ( this.jobMessage.isEmpty() )
        {
            UnsupportedOperationException problem =
                    new UnsupportedOperationException( "Could not execute due to invalid message." );
            LOGGER.warn( "", problem );
            byte[] response =
                    WresEvaluationProcessor.prepareMetaFailureResponse( new UnsupportedOperationException( problem ) );
            this.sendResponse( response );
            return META_FAILURE_CODE;
        }

        // Use one Thread per messenger:
        ExecutorService executorService = Executors.newFixedThreadPool( 3 );

        WebClient.ClientResponse evaluationPostRequest;

        // Open an evaluation for work
        String evaluationId = prepareEvaluationId();

        if ( evaluationId.isEmpty() )
        {
            LOGGER.warn( "Unable to open a new evaluation" );
            byte[] response = WresEvaluationProcessor.prepareResponse( HttpURLConnection.HTTP_BAD_REQUEST,
                                                                       "Failed to open eval" );
            this.sendResponse( response );
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

            URI startEvalURI = URI.create( String.format( START_EVAL_URI, this.getPort(), evaluationId ) );
            evaluationPostRequest =
                    WEB_CLIENT.postToWeb( startEvalURI, jobMessage );

        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Failed to make post request {}", ioe );
            byte[] response = WresEvaluationProcessor.prepareMetaFailureResponse( ioe );
            this.sendResponse( response );
            WresEvaluationProcessor.shutdownExecutor( executorService );
            return META_FAILURE_CODE;
        }

        // Go through the output paths returned from the evaluation post request and send them to the broker
        new BufferedReader( new InputStreamReader( evaluationPostRequest.getResponse() ) ).lines()
                                                                                          .forEach( out -> this.sendOutputPath(
                                                                                                  Paths.get( out ) ) );

        int exitValue = evaluationPostRequest.getStatusCode();
        LOGGER.info( "Request exited with http code: {}", exitValue );
        byte[] response;
        response = WresEvaluationProcessor.prepareResponse( exitValue, null );
        this.sendResponse( response );
        WresEvaluationProcessor.shutdownExecutor( executorService );

        // Close the evaluation before returning
        closeEvaluation();
        return exitValue;
    }

    /**
     * Helper method to prepare an evaluation and create an evaluation ID with the worker server
     * @return String representation of an evaluation id
     * @throws IOException
     */
    private String prepareEvaluationId()
    {
        URI prepareEval = URI.create( String.format( OPEN_EVAL_URI, this.getPort() ) );
        try ( WebClient.ClientResponse evaluationIdRequest = WEB_CLIENT.postToWeb( prepareEval ) )
        {
            if ( evaluationIdRequest.getStatusCode() == HttpURLConnection.HTTP_BAD_REQUEST )
            {
                return "";
            }
            return new BufferedReader( new InputStreamReader( evaluationIdRequest.getResponse() ) ).lines()
                                                                                                   .collect(
                                                                                                           Collectors.joining(
                                                                                                                   "\n" ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
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
            if ( clientResponse.getStatusCode() != HttpsURLConnection.HTTP_OK )
            {
                LOGGER.info( "Evaluation was not able to be closed" );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * Helper to prepare a completed job response
     * @param exitCode the actual exitCode of the wres process
     * @param detail a message with some details about the job completion, for
     *               example the exception that happened, or null if no message.
     * @return raw message indicating job exit code
     */

    private static byte[] prepareResponse( int exitCode, String detail )
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
        return WresEvaluationProcessor.prepareResponse( META_FAILURE_CODE, e.toString() );
    }

    /**
     * Attempts to send a message with job results to the queue specified in job
     * @param message the message to send.
     */
    private void sendResponse( byte[] message )
    {
        AMQP.BasicProperties resultProperties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .deliveryMode( 2 )
                        .build();

        try ( Channel channel = this.getConnection().createChannel() )
        {
            String theExchangeName = this.getExchangeName();
            String exchangeType = "topic";
            String routingKey = "job." + this.getJobId() + ".exitCode";

            channel.exchangeDeclare( theExchangeName, exchangeType, true );

            channel.basicPublish( theExchangeName,
                                  routingKey,
                                  resultProperties,
                                  message );
            LOGGER.info( "Seems like I published to exchange {}, key {}",
                         theExchangeName, routingKey );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this message failed: {}", message, ioe );
        }
        catch ( TimeoutException te )
        {
            LOGGER.warn( "Timed out sending this message: {}", message, te );
        }
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

    /**
     * Attempts to send a message to the broker with a single line of output representing a path of output written by eval
     * @param path the path to send
     */
    private void sendOutputPath( Path path )
    {
        LOGGER.info( "Sending output path {} to broker.", path );
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .deliveryMode( 2 )
                        .build();

        URI theOutputResource = path.toUri();

        LOGGER.info( "Sending output uri {} to broker.", theOutputResource );

        JobOutput.job_output jobOutputMessage = JobOutput.job_output
                .newBuilder()
                .setResource( theOutputResource.toString() )
                .build();
        try ( Channel channel = this.getConnection().createChannel(); )
        {

            channel.basicPublish( this.getExchangeName(),
                                  this.getRoutingKey(),
                                  properties,
                                  jobOutputMessage.toByteArray() );
        }
        catch ( IOException | TimeoutException ioe )
        {
            LOGGER.warn( "Sending this output failed: {}", jobOutputMessage, ioe );
        }
    }
}

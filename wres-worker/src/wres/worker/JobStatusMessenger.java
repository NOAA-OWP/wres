package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.messages.generated.JobStatus.job_status.Report.*;
import static wres.messages.generated.EvaluationStatusOuterClass.EvaluationStatus.*;

import wres.http.RetryPolicy;
import wres.http.WebClient;
import wres.http.WebClientUtils;
import wres.messages.generated.JobStatus;


/**
 * Produces and sends a message indicating the job was RECEIVED, and one ALIVE
 * message for every second (duration) that the process is alive, finally one
 * DEAD message when the process is not alive. Assumes the caller already
 * started the process and that it will be alive before it is dead. Nonetheless,
 * at a bare minimum (assuming connectivity to the broker works), a RECEIVED and
 * DEAD message will be sent even if the process was not started or was dead on
 * arrival to this class.
 *
 * A consumer (e.g. the tasker) can listen for this output on a topic named
 * with the convention job.[job_id].status, for example job.532.status,
 * and can do whatever it chooses with the messages such as display them, serve
 * them, or store them.
 */

public class JobStatusMessenger
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobStatusMessenger.class );
    private static final String TOPIC = "STATUS";

    /** A formatable string to compose the status request to the server */
    private static final String STATUS_URI = "http://localhost:%d/evaluation/status/%s";

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient( WebClientUtils.defaultHttpClient(),
                                                               new RetryPolicy.Builder()
                                                                       .maxRetryTime( Duration.ofSeconds( 30 ) )
                                                                       .maxRetryCount( Integer.MAX_VALUE )
                                                                       .build() );

    private final Connection connection;
    private final String exchangeName;
    private final String jobId;
    private final int port;

    private final String evaluationId;

    /** Helps the consumer re-order messages received out-of-order */
    private final AtomicInteger order = new AtomicInteger( 0 );

    JobStatusMessenger( Connection connection,
                        String exchangeName,
                        String jobId,
                        int port,
                        String evaluationId )
    {
        this.connection = connection;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        this.port = port;
        this.evaluationId = evaluationId;
    }

    private Connection getConnection()
    {
        return this.connection;
    }

    private String getExchangeName()
    {
        return this.exchangeName;
    }

    private String getJobId()
    {
        return this.jobId;
    }

    private int getPort()
    {
        return this.port;
    }

    private AtomicInteger getOrder()
    {
        return this.order;
    }

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + "." + TOPIC;
    }

    private String getEvaluationId()
    {
        return this.evaluationId;
    }

    /**
     * Repeatedly gets and sends messages for a job status until a finish state is reached
     */
    public void run()
    {
        String exchangeType = "topic";

        try ( Channel channel = this.getConnection().createChannel() )
        {
            this.sendMessage( channel, RECEIVED );
            String evaluationStatus = getEvaluationStatus();

            while ( !evaluationStatus.equals( COMPLETED.toString() ) && !evaluationStatus.equals( CLOSED.toString() ) )
            {
                channel.exchangeDeclare( this.getExchangeName(), exchangeType, true );

                this.sendMessage( channel, ALIVE );
                Thread.sleep( 1000 );
                evaluationStatus = getEvaluationStatus();
            }
        }
        catch ( TimeoutException te )
        {
            LOGGER.warn( "Failed to connect to broker.", te );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Failed to create channel, declare exchange, or get status.", ioe );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while checking if process is alive.", ie );
            Thread.currentThread().interrupt();
        }
        LOGGER.info( "Finished sending {} for job {}", TOPIC, jobId );
    }

    /**
     * Method to get the status of an ongoing evaluation
     * @return A string representing an EvaluationStatus
     * @throws IOException exception from WebClient when unable to communicate with server
     */
    private String getEvaluationStatus() throws IOException
    {
        String url = String.format( STATUS_URI, this.getPort(), this.getEvaluationId() );
        try (
                WebClient.ClientResponse fromWeb = WEB_CLIENT.getFromWeb( URI.create( url ),
                                                                          WebClientUtils.getDefaultRetryStates() )
        )
        {
            if ( fromWeb.getStatusCode() == HttpURLConnection.HTTP_OK )
            {
                return new BufferedReader( new InputStreamReader( fromWeb.getResponse() ) ).lines()
                                                                                           .collect( Collectors.joining(
                                                                                                   "\n" ) );
            }

            throw new IOException( "Unable to get status with given ID" );
        }
    }

    /**
     * Attempts to send a message with a single line of output
     * @param channel the channel to use
     * @param report the status/report to send.
     */
    private void sendMessage( Channel channel, JobStatus.job_status.Report report )
    {
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .deliveryMode( 2 )
                        .build();

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                                       .setSeconds( now.getEpochSecond() )
                                       .setNanos( now.getNano() )
                                       .build();
        JobStatus.job_status message
                = JobStatus.job_status
                .newBuilder()
                .setIndex( this.getOrder().getAndIncrement() )
                .setDatetime( timestamp )
                .setReport( report )
                .build();

        try
        {
            channel.basicPublish( this.getExchangeName(),
                                  this.getRoutingKey(),
                                  properties,
                                  message.toByteArray() );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this output failed: {}", message, ioe );
        }
    }
}

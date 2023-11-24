package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.http.WebClient;

/**
 * Produces and sends one message for each line of one of stdout or stderr to
 * the broker, each message containing a line of one of the standard streams.
 *
 * A consumer (e.g. the tasker) can listen for this output on a topic named
 * with the convention job.[job_id].[which_stream], for example job.532.stdout,
 * and can do whatever it chooses with the stream such as display it, serve it,
 * or store it.
 */
public class JobStandardStreamMessenger implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobStandardStreamMessenger.class );
    /** Flag to indicate whether to also send output to system.out, system.err*/
    private static final boolean PASS_THROUGH = true;
    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient();
    /** A formatable string to compose the stdout request to the server */
    private static final String STD_OUT_URI = "http://localhost:%d/evaluation/stdout/%s";
    /** A formatable string to compose the stderr request to the server */
    private static final String STD_ERR_URI = "http://localhost:%d/evaluation/stderr/%s";

    private static final List<Integer> RETRY_STATES = List.of( Response.Status.REQUEST_TIMEOUT.getStatusCode() );

    private static final Duration CALL_TIMEOUT = Duration.ofMinutes( 2 );

    /** Stream identifier. */
    public enum WhichStream
    {
        /** Standard out. */
        STDOUT,
        /** Standard error. */
        STDERR;
    }

    private final Connection connection;
    private final String exchangeName;
    private final String jobId;
    private final String evaluationId;
    private final WhichStream whichStream;
    private final int port;
    /** Helps the consumer re-order the stream */
    private final AtomicInteger order = new AtomicInteger( 0 );

    JobStandardStreamMessenger( Connection connection,
                                String exchangeName,
                                String jobId,
                                WhichStream whichStream,
                                int port,
                                String evaluationId )
    {
        this.connection = connection;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        this.whichStream = whichStream;
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

    private WhichStream getWhichStream()
    {
        return this.whichStream;
    }

    private AtomicInteger getOrder()
    {
        return this.order;
    }

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + "." + this.getWhichStream().name();
    }

    private int getPort()
    {
        return this.port;
    }

    private String getEvaluationId()
    {
        return this.evaluationId;
    }

    @Override
    public void run()
    {
        String url;
        if ( this.getWhichStream().equals( WhichStream.STDOUT ) )
        {
            url = String.format( STD_OUT_URI, this.getPort(), this.getEvaluationId() );
        }
        else
        {
            url = String.format( STD_ERR_URI, this.getPort(), this.getEvaluationId() );
        }

        try (
                WebClient.ClientResponse clientResponse = WEB_CLIENT.getFromWeb( URI.create( url ),
                                                                                 RETRY_STATES,
                                                                                 CALL_TIMEOUT,
                                                                                 true );
                InputStreamReader utf8Reader = new InputStreamReader( clientResponse.getResponse(),
                                                                      StandardCharsets.UTF_8 );
                BufferedReader reader = new BufferedReader( utf8Reader );
                Channel channel = this.getConnection().createChannel() )
        {
            LOGGER.info( "Established redirect for {} ", this.getWhichStream() );
            String exchangeName = this.getExchangeName();
            String exchangeType = "topic";
            channel.exchangeDeclare( exchangeName, exchangeType, true );
            reader.lines()
                  .forEach( line -> this.sendLine( channel, line ) );
        }
        catch ( TimeoutException te )
        {
            LOGGER.warn( "Failed to connect to broker.", te );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Failed to read a line,", ioe );
        }
        LOGGER.info( "Finished sending {} for job {}", whichStream, jobId );
    }

    /**
     * Attempts to send a message with a single line of output
     * @param channel the channel to use
     * @param line the line to send.
     */
    private void sendLine( Channel channel, String line )
    {
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .deliveryMode( 2 )
                        .build();
        int order = this.getOrder().getAndIncrement();
        wres.messages.generated.JobStandardStream.job_standard_stream message
                = wres.messages.generated.JobStandardStream.job_standard_stream
                .newBuilder()
                .setIndex( order )
                .setText( line )
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
            LOGGER.error( "Sending this output failed: {}", message, ioe );
        }
        // We may also wish to see output on standard out and standard err...
        if ( PASS_THROUGH && this.getWhichStream().equals( WhichStream.STDOUT ) )
        {
            LOGGER.info( line );
        }
        else if ( PASS_THROUGH
                  && this.getWhichStream().equals( WhichStream.STDERR ) )
        {
            LOGGER.error( line );
        }
    }
}
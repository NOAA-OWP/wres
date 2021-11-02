package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    public enum WhichStream
    {
        STDOUT,
        STDERR;
    }

    private final Connection connection;
    private final String exchangeName;
    private final String jobId;
    private final WhichStream whichStream;
    private final InputStream stream;

    /** Helps the consumer re-order the stream */
    private final AtomicInteger order = new AtomicInteger( 0 );

    JobStandardStreamMessenger( Connection connection,
                                String exchangeName,
                                String jobId,
                                WhichStream whichStream,
                                InputStream stream )
    {
        this.connection = connection;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        this.whichStream = whichStream;
        this.stream = stream;
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

    private InputStream getStream()
    {
        return this.stream;
    }

    private AtomicInteger getOrder()
    {
        return this.order;
    }

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + "." + this.getWhichStream().name();
    }

    @Override
    public void run()
    {
        InputStream inputStream = this.getStream();
        try ( InputStreamReader utf8Reader = new InputStreamReader( inputStream,
                                                                    StandardCharsets.UTF_8 );
              BufferedReader reader = new BufferedReader( utf8Reader );
              Channel channel = this.getConnection().createChannel() )
        {
            String exchangeName = this.getExchangeName();
            String exchangeType = "topic";

            channel.exchangeDeclare( exchangeName, exchangeType );

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
            LOGGER.warn( "Sending this output failed: {}", message, ioe );
        }

        // We may also wish to see output on standard out and standard err...
        if ( PASS_THROUGH && this.getWhichStream().equals( WhichStream.STDOUT ) )
        {
            System.out.println( line );
        }
        else if ( PASS_THROUGH
                  && this.getWhichStream().equals( WhichStream.STDERR ) )
        {
            System.err.println( line );
        }
    }
}

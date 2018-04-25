package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputMessenger implements Consumer<InputStream>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( OutputMessenger.class );

    /** Flag to indicate whether to also send output to system.out, system.err*/
    private static final boolean PASS_THROUGH = true;

    public enum WhichOutput
    {
        STDOUT,
        STDERR;
    }

    private final Connection connection;
    private final String exchangeName;
    private final String jobId;
    private final WhichOutput whichOutput;

    OutputMessenger( Connection connection,
                     String exchangeName,
                     String jobId,
                     WhichOutput whichOutput )
    {
        this.connection = connection;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        this.whichOutput = whichOutput;
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

    private WhichOutput getWhichOutput()
    {
        return this.whichOutput;
    }

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + "." + this.getWhichOutput().name();
    }

    @Override
    public void accept( InputStream inputStream )
    {
        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( inputStream ) );
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

        try
        {
            channel.basicPublish( this.getExchangeName(),
                                  this.getRoutingKey(),
                                  properties,
                                  line.getBytes( Charset.forName( "UTF-8" ) ) );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this output failed: {}", line, ioe );
        }

        // We may also wish to see output on standard out and standard err...
        if ( PASS_THROUGH && this.getWhichOutput().equals( WhichOutput.STDOUT ) )
        {
            System.out.println( line );
        }
        else if ( PASS_THROUGH
                  && this.getWhichOutput().equals( WhichOutput.STDERR ) )
        {
            System.err.println( line );
        }
    }
}

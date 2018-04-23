package wres.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputMessenger implements Consumer<InputStream>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( OutputMessenger.class );

    private final ConnectionFactory connectionFactory;
    private final String exchangeName;
    private final String jobId;

    OutputMessenger( ConnectionFactory connectionFactory,
                     String exchangeName,
                     String jobId )
    {
        this.connectionFactory = connectionFactory;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
    }

    private ConnectionFactory getConnectionFactory()
    {
        return this.connectionFactory;
    }

    private String getExchangeName()
    {
        return this.exchangeName;
    }

    private String getJobId()
    {
        return this.jobId;
    }

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + ".stdout";
    }

    @Override
    public void accept( InputStream inputStream )
    {
        String line;

        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( inputStream ) );
              Connection connection = this.getConnectionFactory().newConnection();
              Channel channel = connection.createChannel() )
        {
            String exchangeName = this.getExchangeName();
            String exchangeType = "topic";

            channel.exchangeDeclare( exchangeName, exchangeType );

            do
            {
                line = reader.readLine();
                this.sendLine( channel, line );
            }
            while ( line != null );
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
                                  line.getBytes() );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this output failed: {}", line, ioe );
        }
    }
}

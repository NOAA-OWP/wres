package wres.tasker;

import java.util.concurrent.BlockingQueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Looks on channel for messages matching given correlationId and puts results
 * into a (java) queue specified by the caller.
 */

public class JobResultReceiver extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobResultReceiver.class );

    private final String correlationId;
    private final BlockingQueue<Integer> result;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * @param channel the channel to which this consumer is attached
     * @param correlationId the correlation id to look for
     * @param result the shared object to write a result to
     */
    JobResultReceiver( Channel channel,
                       String correlationId,
                       BlockingQueue<Integer> result )
    {
        super( channel );
        this.correlationId = correlationId;
        this.result = result;
    }

    private String getCorrelationId()
    {
        return this.correlationId;
    }

    private BlockingQueue<Integer> getResult()
    {
        return this.result;
    }

    @Override
    public void handleDelivery( String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] message )
    {
        if ( properties.getCorrelationId().equals( this.getCorrelationId() ) )
        {
            LOGGER.info( "Found a message with matching correlationId {}=={}",
                         properties.getCorrelationId(), this.getCorrelationId() );
            // TODO: deserialize the message here, read the actual result value
            this.getResult().offer( 1 );
        }
        else
        {
            LOGGER.info( "Found a message with non-matching correlationIds {}!={}",
                         properties.getCorrelationId(), this.getCorrelationId() );
        }
    }
}

package wres.tasker;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobOutputReceiver extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobOutputReceiver.class );

    private final BlockingQueue<String> result;

    /**
     * @param channel the channel to which this consumer is attached
     * @param result the shared object to write a result to
     */

    JobOutputReceiver( Channel channel,
                       BlockingQueue<String> result )
    {
        super( channel );
        this.result = result;
    }

    private BlockingQueue<String> getResult()
    {
        return this.result;
    }

    @Override
    public void handleDelivery( String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] message )
    {
        LOGGER.debug( "Heard a message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                      consumerTag, envelope, properties, message );
        String decodedResult = new String( message, Charset.forName( "UTF-16" ) );
        this.getResult().offer( decodedResult );
    }
}

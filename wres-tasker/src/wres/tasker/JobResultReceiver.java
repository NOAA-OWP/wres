package wres.tasker;

import java.util.concurrent.BlockingQueue;

import com.google.protobuf.InvalidProtocolBufferException;
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

    private static final Integer PARSE_FAILED = 601;

    private final BlockingQueue<Integer> result;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * @param channel the channel to which this consumer is attached
     * @param result the shared object to write a result to
     */
    JobResultReceiver( Channel channel,
                       BlockingQueue<Integer> result )
    {
        super( channel );
        this.result = result;
        LOGGER.debug( "instantiated with channel {} and result {}",
                      channel, result );
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
        LOGGER.info( "Heard a message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                     consumerTag, envelope, properties, message );
        wres.messages.generated.JobResult.job_result jobResult;

        try
        {
            jobResult = wres.messages.generated.JobResult.job_result.parseFrom(
                    message );
            this.getResult().offer( jobResult.getResult() );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            LOGGER.warn( "Could not parse a job result message.", ipbe );
            this.getResult().offer( PARSE_FAILED );
        }
    }
}

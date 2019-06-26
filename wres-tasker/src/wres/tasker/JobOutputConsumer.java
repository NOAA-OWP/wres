package wres.tasker;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.JobOutput;

class JobOutputConsumer extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobOutputConsumer.class );

    private static final Duration OFFER_LIMIT = Duration.ofSeconds( 30 );

    private final BlockingQueue<JobOutput.job_output> wresJobOutput;

    JobOutputConsumer( Channel channel,
                       BlockingQueue<JobOutput.job_output> wresJobOutput )
    {
        super( channel );
        this.wresJobOutput = wresJobOutput;
    }

    private BlockingQueue<JobOutput.job_output> getWresJobOutput()
    {
        return this.wresJobOutput;
    }

    @Override
    public void handleDelivery( String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] message )

    {
        LOGGER.debug( "Heard a message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                      consumerTag, envelope, properties, message );

        JobOutput.job_output oneWresJobOutputMessage = null;

        try
        {
            oneWresJobOutputMessage = JobOutput.job_output.parseFrom( message );
            LOGGER.debug( "Successfully parsed message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                          consumerTag, envelope, properties, oneWresJobOutputMessage );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            // Not much we can do at this point.
            LOGGER.warn( "Could not parse a job output message.", ipbe );
        }

        try
        {
            boolean offerSucceeded = this.getWresJobOutput()
                                         .offer( oneWresJobOutputMessage,
                                                 OFFER_LIMIT.getSeconds(),
                                                 TimeUnit.SECONDS );

            if ( !offerSucceeded )
            {
                LOGGER.warn( "Failed to offer job output message {} after {}, gave up.",
                             message, OFFER_LIMIT );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while attempting to put job output message "
                         + Arrays.toString( message ), ie );
            Thread.currentThread().interrupt();
        }

    }

}

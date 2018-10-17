package wres.tasker;

import java.util.concurrent.BlockingQueue;

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

        JobOutput.job_output oneWresJobOutputMessage;

        try
        {
            oneWresJobOutputMessage = JobOutput.job_output.parseFrom( message );

            LOGGER.debug( "Successfully parsed message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                          consumerTag, envelope, properties, oneWresJobOutputMessage );
            boolean offerSucceeded = this.getWresJobOutput().offer( oneWresJobOutputMessage );

            if ( !offerSucceeded )
            {
                LOGGER.info( "Failed to offer job output message {}, trying again",
                             message );
                boolean secondOfferSucceeded = this.getWresJobOutput()
                                                   .offer( oneWresJobOutputMessage );
                if ( !secondOfferSucceeded )
                {
                    LOGGER.warn( "Failed again to offer job output message {}, gave up.",
                                 message );
                }
            }
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            // Not much we can do at this point.
            LOGGER.warn( "Could not parse a job output message.", ipbe );
        }
    }

}

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

import wres.messages.generated.JobStatus;

class JobStatusConsumer extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobOutputConsumer.class );

    private static final Duration OFFER_LIMIT = Duration.ofSeconds( 30 );

    private final BlockingQueue<JobStatus.job_status> wresJobStatus;

    JobStatusConsumer( Channel channel,
                       BlockingQueue<JobStatus.job_status> wresJobStatus )
    {
        super( channel );
        this.wresJobStatus = wresJobStatus;
    }

    private BlockingQueue<JobStatus.job_status> getWresJobStatus()
    {
        return this.wresJobStatus;
    }

    @Override
    public void handleDelivery( String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] message )

    {
        LOGGER.debug( "Heard a job status message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                      consumerTag, envelope, properties, message );

        JobStatus.job_status oneWresJobStatusMessage = null;

        try
        {
            oneWresJobStatusMessage = JobStatus.job_status.parseFrom( message );
            LOGGER.debug( "Successfully parsed job status message, consumerTag: {}, envelope: {}, properties: {}, message: {}",
                          consumerTag, envelope, properties, oneWresJobStatusMessage );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            // Not much we can do at this point.
            LOGGER.warn( "Could not parse a job status message.", ipbe );
        }

        try
        {
            boolean offerSucceeded = this.getWresJobStatus()
                                         .offer( oneWresJobStatusMessage,
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

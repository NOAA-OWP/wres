package wres.tasker;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.JobStatus;

class JobStatusWatcher implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobStatusWatcher.class );

    private static final String TOPIC = "status";
    private static final int LOCAL_Q_SIZE = 5;

    /**
     * The shared connection to the broker to use for communication.
     */
    private final Connection connection;

    /**
     * The exchange on the broker to bind to for data about WRES evaluation jobs
     */
    private final String jobStatusExchangeName;

    /**
     * The job to get the id from and also store URIs to.
     */
    private final JobMetadata jobMetadata;


    JobStatusWatcher( Connection connection,
                      String jobStatusExchangeName,
                      JobMetadata jobMetadata )
    {
        Objects.requireNonNull( connection );
        Objects.requireNonNull( jobStatusExchangeName );
        Objects.requireNonNull( jobMetadata );
        this.connection = connection;
        this.jobStatusExchangeName = jobStatusExchangeName;
        this.jobMetadata = jobMetadata;
    }

    private Connection getConnection()
    {
        return this.connection;
    }

    private String getJobStatusExchangeName()
    {
        return this.jobStatusExchangeName;
    }

    private String getJobId()
    {
        return this.jobMetadata.getId();
    }

    private JobMetadata getJobMetadata()
    {
        return this.jobMetadata;
    }

    @Override
    public void run()
    {
        Consumer<GeneratedMessageV3> sharer = new JobStatusSharer( this.getJobMetadata() );

        BlockingQueue<JobStatus.job_status> jobStatusQueue
                = new ArrayBlockingQueue<>( LOCAL_Q_SIZE );

        String exchangeName = this.getJobStatusExchangeName();
        String exchangeType = "topic";
        String bindingKey = "job." + this.getJobId() + "." + TOPIC;

        try ( Channel channel = this.getConnection().createChannel() )
        {
            channel.exchangeDeclare( exchangeName, exchangeType );

            // As the consumer, I want an exclusive queue for me.
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind( queueName, exchangeName, bindingKey );

            JobStatusConsumer jobStatusConsumer =
                    new JobStatusConsumer( channel, jobStatusQueue );

            channel.basicConsume( queueName,
                                  true,
                                  jobStatusConsumer );

            JobMessageHelper.waitForAllMessages( queueName,
                                                 this.getJobId(),
                                                 jobStatusQueue,
                                                 sharer,
                                                 TOPIC );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting status for job {}",
                         this.getJobId(), ie );
            Thread.currentThread().interrupt();
        }
        catch ( IOException | TimeoutException e )
        {
            // Since we may or may not actually consume result, log exception here
            LOGGER.warn( "When attempting to get job status message using {}:",
                         this, e );
        }
    }
}

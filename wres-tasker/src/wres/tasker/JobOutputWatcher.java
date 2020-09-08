package wres.tasker;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.JobOutput;

class JobOutputWatcher implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobOutputWatcher.class );

    private static final String TOPIC = "output";
    private static final int LOCAL_Q_SIZE = 100;

    /**
     * The shared connection to the broker to use for communication.
     */
    private final Connection connection;

    /**
     * The exchange on the broker to bind to for data about WRES evaluation jobs
     */
    private final String jobStatusExchangeName;

    /**
     * The id of a WRES evaluation job (as defined by tasker)
     */
    private final String jobId;

    /**
     * Shared mutable state, where to put all the URIs from job output messages
     */
    private final Set<URI> sharedUris;

    JobOutputWatcher( Connection connection,
                      String jobStatusExchangeName,
                      String jobId,
                      Set<URI> sharedUris )
    {
        Objects.requireNonNull( connection );
        Objects.requireNonNull( jobStatusExchangeName );
        Objects.requireNonNull( jobId );
        Objects.requireNonNull( sharedUris );
        this.connection = connection;
        this.jobStatusExchangeName = jobStatusExchangeName;
        this.jobId = jobId;
        this.sharedUris = sharedUris;
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
        return this.jobId;
    }

    private Set<URI> getSharedUris()
    {
        return this.sharedUris;
    }

    @Override
    public void run()
    {
        Consumer<GeneratedMessageV3> sharer = new JobOutputSharer( this.getSharedUris() );

        BlockingQueue<JobOutput.job_output> jobOutputQueue
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

            JobOutputConsumer jobOutputConsumer =
                    new JobOutputConsumer( channel, jobOutputQueue );

            channel.basicConsume( queueName,
                                  true,
                                  jobOutputConsumer );

            JobMessageHelper.waitForAllMessages( queueName,
                                                 this.getJobId(),
                                                 jobOutputQueue,
                                                 sharer,
                                                 TOPIC );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting output for job {}", jobId, ie );
            Thread.currentThread().interrupt();
        }
        catch ( IOException | TimeoutException e )
        {
            // Since we may or may not actually consume result, log exception here
            LOGGER.warn( "When attempting to get job output message using {}:",
                         this, e );
        }
    }
}

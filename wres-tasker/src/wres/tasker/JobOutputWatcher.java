package wres.tasker;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.JobOutput;

class JobOutputWatcher implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobOutputWatcher.class );

    private static final String TOPIC = "OUTPUT";
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
     * The job to get the id from and also store URIs to.
     */
    private final JobMetadata jobMetadata;

    /**
     * A latch to count down once this is actually watching.
     */
    private final CountDownLatch countDownLatch;

    JobOutputWatcher( Connection connection,
                      String jobStatusExchangeName,
                      JobMetadata jobMetadata,
                      CountDownLatch countDownLatch )
    {
        Objects.requireNonNull( connection );
        Objects.requireNonNull( jobStatusExchangeName );
        Objects.requireNonNull( jobMetadata );
        Objects.requireNonNull( countDownLatch );
        this.connection = connection;
        this.jobStatusExchangeName = jobStatusExchangeName;
        this.jobMetadata = jobMetadata;
        this.countDownLatch = countDownLatch;
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

    private CountDownLatch getCountDownLatch()
    {
        return this.countDownLatch;
    }

    @Override
    public void run()
    {
        Consumer<GeneratedMessageV3> sharer = new JobOutputSharer( this.getJobMetadata() );

        BlockingQueue<JobOutput.job_output> jobOutputQueue
                = new ArrayBlockingQueue<>( LOCAL_Q_SIZE );

        String exchangeName = this.getJobStatusExchangeName();
        String exchangeType = "topic";
        String bindingKey = "job." + this.getJobId() + "." + TOPIC;
        String queueName = null;
        Channel channel = null;

        try
        {
            channel = this.getConnection().createChannel();
            if ( Objects.isNull( channel ) )
            {
                LOGGER.warn( "Channel was unable to be created. There might be a leak" );
                return;
            }

            channel.exchangeDeclare( exchangeName, exchangeType, true );

            // As the consumer, I want an exclusive queue for me.
            queueName = channel.queueDeclare(bindingKey, true, false, false, null).getQueue();
            channel.queueBind( queueName, exchangeName, bindingKey );

            LOGGER.info("Watching the queue {} for job output.", queueName);

            JobOutputConsumer jobOutputConsumer =
                    new JobOutputConsumer( channel, jobOutputQueue );

            channel.basicConsume( queueName,
                                  true,
                                  jobOutputConsumer );
            this.getCountDownLatch().countDown();
            JobMessageHelper.waitForAllMessages( queueName,
                                                 this.getJobId(),
                                                 jobOutputQueue,
                                                 sharer,
                                                 TOPIC );

        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting output for job {}",
                         this.getJobId(), ie );
            Thread.currentThread().interrupt();
        }
        catch ( IOException e )
        {
            // Since we may or may not actually consume result, log exception here
            LOGGER.warn( "When attempting to get job output message using {}:",
                         this, e );
        }
        finally
        {
            try
            {
                if ( queueName != null )
                {
                    LOGGER.info( "Deleting the queue {}", queueName );
                    AMQP.Queue.DeleteOk deleteOk = channel.queueDelete( queueName );
                    if ( deleteOk == null )
                    {
                        LOGGER.warn( "Delete queue with name {} failed. There might be a zombie queue.",
                                     queueName );
                    }
                }

                if ( channel != null )
                {
                    channel.close();
                }
            }
            catch ( IOException | TimeoutException e )
            {
                LOGGER.warn(
                        "Delete queue with name {} failed or unable to close channel due to an exception. There might be a zombie queue.",
                        queueName,
                        e );
            }
        }

    }
}

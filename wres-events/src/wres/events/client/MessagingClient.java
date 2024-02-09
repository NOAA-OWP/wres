package wres.events.client;

import java.io.IOException;
import java.io.Serial;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import wres.events.broker.BrokerConnectionFactory;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.SubscriberStatus;
import wres.events.subscribe.UnrecoverableSubscriberException;

/**
 * A long-running messaging client that encapsulates one subscriber, which consumes statistics. Compose an
 * instance of this class within an application class. Call {@link #start()} to start the client, {@link #await()} to
 * make the client wait indefinitely for messaging work to complete and {@link #stop()} to stop the client.
 * Alternatively, to run the client inband to (i.e., in the same thread as) messaging work, call {@link #start()} to
 * start the client and {@link #stop()} to stop the client. In short, {@link #await()} should be used only when the
 * client is started in a different thread than the messaging work.
 *
 * @author James Brown
 */

public class MessagingClient
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MessagingClient.class );

    /** The frequency with which to log the status of the server in ms.*/
    private static final long STATUS_UPDATE_MILLISECONDS = 60_000;

    /** The frequency with which to check the health of the subscriber in milliseconds. */
    private static final long HEALTH_CHECK_MILLISECONDS = 5_000;

    /** Maximum number of threads. */
    private static final int MAXIMUM_THREAD_COUNT = 5;

    /** Client version to report. */
    private static final Version VERSION = new Version();

    /** An executor to execute the server. */
    private final ExecutorService clientExecutor;

    /** An executor to do the work. */
    private final ExecutorService worker;

    /** A timer task to print information about the status of the server. */
    private final Timer timer;

    /** A latch to make the client wait for messaging work when the client is started in a separate thread. */
    private final CountDownLatch latch;

    /** The subscriber. */
    private final EvaluationSubscriber subscriber;

    /** The consumer factory. */
    private final ConsumerFactory consumerFactory;

    /** Is {@code true} if the client has been closed, otherwise {@code false}. */
    private final AtomicBoolean isClosed;

    /** The time at which the client was started. */
    private Instant started;

    /**
     * Creates an instance.
     * @param broker the broker
     * @param consumerFactory the consumer factory
     * @return an instance of the server
     * @throws NullPointerException if any input is null
     * @throws MessagingClientException if the messaging client could not be created
     */

    public static MessagingClient of( BrokerConnectionFactory broker,
                                      ConsumerFactory consumerFactory )
    {
        return new MessagingClient( broker, consumerFactory );
    }

    /**
     * @return the client status.
     */

    public SubscriberStatus getSubscriberStatus()
    {
        return this.getSubscriber()
                   .getSubscriberStatus();
    }

    /**
     * Starts the client.
     */

    public void start()
    {
        LOGGER.info( "Starting WRES messaging client {}...", this );

        // Print version information
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        String processId = StringUtils.substringBefore( processName, "@" );

        MDC.put( "pid", processId );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Process: {}", processId );
            LOGGER.info( MessagingClient.VERSION.getDescription() );
            LOGGER.info( MessagingClient.VERSION.getVerboseRuntimeDescription() );
        }

        this.clientExecutor.submit( this::run );
    }

    /**
     * Asks the client to wait indefinitely for the completion of messaging tasks.
     * @throws InterruptedException if the server is interrupted.
     */

    public void await() throws InterruptedException
    {
        this.latch.await();
    }

    /**
     * Stops the client.
     */
    public void stop()
    {
        // Not closed already?
        if ( !this.isClosed.getAndSet( true ) )
        {
            LOGGER.info( "Stopping WRES messaging client {}...", this );

            this.timer.cancel();

            if ( Objects.nonNull( this.consumerFactory ) )
            {
                try
                {
                    this.consumerFactory.close();
                }
                catch ( IOException e )
                {
                    LOGGER.error( "While closing messaging client {}, failed to close a statistics consumer factory.",
                                  this );
                }
            }

            if ( Objects.nonNull( this.subscriber ) )
            {
                try
                {
                    this.subscriber.close();
                }
                catch ( IOException e )
                {
                    if ( LOGGER.isWarnEnabled() )
                    {
                        String message = "Failed to close subscriber " + this.subscriber.getClientId() + ".";
                        LOGGER.warn( message, e );
                    }
                }
            }

            this.closeExecutors();

            this.latch.countDown();

            Instant ended = Instant.now();
            Duration duration = Duration.between( started, ended );

            LOGGER.info( "Stopped WRES messaging client {} at {}. The client ran for '{}' and processed {} packets of "
                         + "statistics across {} evaluations.",
                         this,
                         ended,
                         duration,
                         this.getSubscriberStatus()
                             .getStatisticsCount(),
                         this.getSubscriberStatus()
                             .getEvaluationCount() );
        }
    }

    @Override
    public String toString()
    {
        return this.getClientId();
    }

    /**
     * Creates a timer task, which tracks the status of the server.
     */

    private void run()
    {
        // The status is mutable and is updated by the subscriber
        SubscriberStatus status = this.getSubscriberStatus();
        MessagingClient client = this;

        // Create a timer task to log the server status
        TimerTask updater = new TimerTask()
        {
            @Override
            public void run()
            {
                // Log status
                LOGGER.info( "{}", status );
            }
        };

        // Create a timer task to check on the health of the subscriber
        TimerTask healthChecker = new TimerTask()
        {
            @Override
            public void run()
            {
                if ( status.isFailed() )
                {
                    LOGGER.error( "While checking messaging client {} for the health of its subscribers, "
                                  + "discovered a failed subscriber. The messaging client will now "
                                  + "close.",
                                  client );

                    client.stop();
                }
            }
        };

        this.timer.schedule( updater, 0, MessagingClient.STATUS_UPDATE_MILLISECONDS );
        this.timer.schedule( healthChecker, 0, MessagingClient.HEALTH_CHECK_MILLISECONDS );

        this.started = Instant.now();

        LOGGER.info( "WRES messaging client {} started successfully at {}.", this, this.started );
    }

    /**
     * Closes the executors that serve this client.
     */

    private void closeExecutors()
    {
        this.getWorker()
            .shutdown();

        try
        {
            boolean terminated = this.getWorker()
                                     .awaitTermination( 5, TimeUnit.SECONDS );

            if ( !terminated )
            {
                List<Runnable> tasks = this.getWorker()
                                           .shutdownNow();

                if ( !tasks.isEmpty() && LOGGER.isInfoEnabled() )
                {
                    LOGGER.info( "Abandoned {} tasks from {}",
                                 tasks.size(),
                                 this.getWorker() );
                }
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while shutting down the executor service {} in WRES messaging "
                         + "client {}.",
                         this.getWorker(),
                         this );

            Thread.currentThread().interrupt();
        }

        this.getClientExecutor()
            .shutdown();

        try
        {
            boolean terminated = this.getClientExecutor()
                                     .awaitTermination( 5, TimeUnit.SECONDS );

            if ( !terminated )
            {
                List<Runnable> tasks = this.getClientExecutor()
                                           .shutdownNow();

                if ( !tasks.isEmpty() && LOGGER.isInfoEnabled() )
                {
                    LOGGER.info( "Abandoned {} tasks from {}",
                                 tasks.size(),
                                 this.getClientExecutor() );
                }
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Failed to close all resources used by WRES messaging client {}.", this );

            Thread.currentThread().interrupt();
        }
    }

    /**
     * @return the subscriber.
     */

    private EvaluationSubscriber getSubscriber()
    {
        return this.subscriber;
    }

    /**
     * @return the subscriber identifier.
     */

    private String getClientId()
    {
        return this.getSubscriber()
                   .getClientId();
    }

    /**
     * @return the executor that performs the message consumption.
     */

    private ExecutorService getWorker()
    {
        return this.worker;
    }

    /**
     * @return the executor that runs the service.
     */

    private ExecutorService getClientExecutor()
    {
        return this.clientExecutor;
    }

    /**
     * Do not construct.
     * @param broker the broker
     * @param consumerFactory the consumer factory
     * @throws NullPointerException if any input is null
     * @throws MessagingClientException if the messaging client could not be created
     */

    private MessagingClient( BrokerConnectionFactory broker,
                             ConsumerFactory consumerFactory )
    {
        Objects.requireNonNull( broker );
        Objects.requireNonNull( consumerFactory );

        String clientId = consumerFactory.getConsumerDescription()
                                         .getConsumerId();

        LOGGER.info( "Creating WRES messaging client {}...", clientId );

        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an internal error in WRES messaging client " + clientId + ".";
            LOGGER.error( message, b );
            this.stop();
        };

        ThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern( "Messaging Client "
                                                                                      + clientId
                                                                                      + " Thread %d" )
                                                                      .uncaughtExceptionHandler( handler )
                                                                      .build();

        this.clientExecutor = Executors.newSingleThreadExecutor( threadFactory );
        this.timer = new Timer( true );
        this.latch = new CountDownLatch( 1 );
        this.worker = this.createWorker( clientId );
        this.isClosed = new AtomicBoolean();

        // A factory that creates consumers on demand
        this.consumerFactory = consumerFactory;

        try
        {
            this.subscriber = EvaluationSubscriber.of( this.consumerFactory,
                                                       this.getWorker(),
                                                       broker,
                                                       false );
            this.subscriber.start();
        }
        catch ( UnrecoverableSubscriberException e )
        {
            throw new MessagingClientException( "While attempting to build a WRES messaging client, encountered an "
                                                + "internal error.",
                                                e );
        }

        LOGGER.info( "Finished creating a WRES messaging client with subscriber identifier {}.", this );
    }

    /**
     * Creates an executor service to perform the message consumption work.
     * @param clientId the client identifier
     * @return the created service
     */

    private ExecutorService createWorker( String clientId )
    {
        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an internal error in a WRES worker of the WRES messaging client "
                             + clientId
                             + ".";
            LOGGER.error( message, b );
            this.stop();
        };

        ThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern( "Messaging Client "
                                                                                      + clientId
                                                                                      + " Worker Thread %d" )
                                                                      .uncaughtExceptionHandler( handler )
                                                                      .build();

        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>( MessagingClient.MAXIMUM_THREAD_COUNT * 5 );

        ThreadPoolExecutor pool = new ThreadPoolExecutor( MessagingClient.MAXIMUM_THREAD_COUNT,
                                                          MessagingClient.MAXIMUM_THREAD_COUNT,
                                                          MessagingClient.MAXIMUM_THREAD_COUNT,
                                                          TimeUnit.MILLISECONDS,
                                                          workQueue,
                                                          threadFactory );

        // Punt to the main thread to slow progress when rejected
        pool.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );

        return pool;
    }

    /**
     * Messaging client exception.
     */
    public static class MessagingClientException extends RuntimeException
    {
        /**
         * Serial identifier.
         */
        @Serial
        private static final long serialVersionUID = -3496487018421078900L;

        /**
         * Builds a {@link MessagingClientException} with the specified message.
         *
         * @param message the message.
         * @param cause the cause of the exception
         */

        public MessagingClientException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

}

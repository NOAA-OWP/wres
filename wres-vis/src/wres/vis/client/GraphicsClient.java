package wres.vis.client;

import java.io.IOException;
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

import wres.events.EvaluationEventUtilities;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.SubscriberStatus;
import wres.events.subscribe.UnrecoverableSubscriberException;
import wres.eventsbroker.BrokerConnectionFactory;

/**
 * A long-running graphics client that encapsulates one graphics subscriber, which consumes statistics and writes them 
 * to graphics.
 * 
 * @author james.brown@hydrosolved.com
 */

class GraphicsClient
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsClient.class );

    /**
     * The frequency with which to log the status of the server in ms.
     */

    private static final long STATUS_UPDATE_MILLISECONDS = 60_000;

    /**
     * The frequency with which to check the health of the subscriber in ms.
     */

    private static final long HEALTH_CHECK_MILLISECONDS = 5_000;

    /**
     * Maximum number of threads for graphics writing.
     */

    private static final int MAXIMUM_THREAD_COUNT = 5;

    /**
     * Software version.
     */

    private static final Version VERSION = new Version();

    /**
     * An executor to execute the graphics server.
     */

    private final ExecutorService clientExecutor;

    /**
     * An executor to do the graphics work.
     */

    private final ExecutorService graphicsWorker;

    /**
     * A timer task to print information about the status of the server.
     */

    private final Timer timer;

    /**
     * Wait until stopped.
     */

    private final CountDownLatch latch;

    /**
     * The graphics subscriber;
     */

    private final EvaluationSubscriber graphicsSubscriber;

    /**
     * The consumer factory.
     */

    private final ConsumerFactory consumerFactory;

    /**
     * Is {@code true} if the client has been closed, otherwise {@code false}.
     */

    private final AtomicBoolean isClosed;

    /**
     * Start the graphics server.
     * @param args the command line arguments
     */

    public static void main( String[] args )
    {
        // Print version information
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        String processId = StringUtils.substringBefore( processName, "@" );

        MDC.put( "pid", processId );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Process: {}", processId );
            LOGGER.info( GraphicsClient.VERSION.getDescription() );
            LOGGER.info( GraphicsClient.VERSION.getVerboseRuntimeDescription() );
        }

        // Create the server
        int exitCode = 0;

        BrokerConnectionFactory broker = BrokerConnectionFactory.of( false ); // No dynamic binding, nominated port only
        GraphicsClient graphics = GraphicsClient.of( broker );

        Instant started = Instant.now();

        // Add a shutdown hook to respond gracefully to SIGINT signals
        Runtime.getRuntime()
               .addShutdownHook( new Thread( () -> {

                   LOGGER.info( "Closing WRES Graphics Client {}...", graphics );

                   // Close the resources
                   graphics.stop();

                   try
                   {
                       LOGGER.info( "Closing broker connections {}.", broker );
                       broker.close();
                   }
                   catch ( IOException e )
                   {
                       LOGGER.error( "Failed to close the broker connections associated with graphics client {}.",
                                     graphics );

                   }

                   Instant ended = Instant.now();
                   Duration duration = Duration.between( started, ended );

                   LOGGER.info( "Closed WRES Graphics Client {}, which ran for '{}' and processed {} packets of "
                                + "statistics across {} evaluations.",
                                graphics,
                                duration,
                                graphics.getSubscriberStatus().getStatisticsCount(),
                                graphics.getSubscriberStatus().getEvaluationCount() );
               } ) );

        try
        {
            // Start the subscriber
            graphics.start();

            // Await termination of the client
            graphics.await();
        }
        catch ( InterruptedException e )
        {
            LOGGER.error( "Interrupted while waiting for a WRES Graphics Client." );

            exitCode = 1;

            Thread.currentThread().interrupt();

        }
        catch ( GraphicsClientException f )
        {
            LOGGER.error( "Encountered an internal error in a WRES Graphics Client, which will now shut down.", f );

            exitCode = 1;
        }

        System.exit( exitCode );
    }

    @Override
    public String toString()
    {
        return this.getClientId();
    }

    /**
     * Creates an instance.
     * @param broker the broker
     * @return an instance of the server
     */

    static GraphicsClient of( BrokerConnectionFactory broker )
    {
        return new GraphicsClient( broker );
    }

    /**
     * Starts the client.
     */

    void start()
    {
        this.clientExecutor.submit( this::run );
    }

    /**
     * Stops the client.
     */
    void stop()
    {
        // Not closed already?
        if ( !this.isClosed.getAndSet( true ) )
        {
            this.timer.cancel();

            if ( Objects.nonNull( this.consumerFactory ) )
            {
                try
                {
                    this.consumerFactory.close();
                }
                catch ( IOException e )
                {
                    LOGGER.error( "While closing graphics client {}, failed to close a statistics consumer factory.",
                                  this );
                }
            }

            if ( Objects.nonNull( this.graphicsSubscriber ) )
            {
                try
                {
                    this.graphicsSubscriber.close();
                }
                catch ( IOException e )
                {
                    if ( LOGGER.isWarnEnabled() )
                    {
                        String message = "Failed to close subscriber " + this.graphicsSubscriber.getClientId() + ".";
                        LOGGER.warn( message, e );
                    }
                }
            }

            this.closeExecutors();

            this.latch.countDown();
        }
    }

    /**
     * Creates a timer task, which tracks the status of the server.
     */

    private void run()
    {
        LOGGER.info( "WRES Graphics client {} is running.", this );

        // The status is mutable and is updated by the subscriber
        SubscriberStatus status = this.getSubscriberStatus();
        GraphicsClient client = this;

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
                    LOGGER.error( "While checking the graphics client for the health of its subscribers, discovered a "
                                  + "failed subscriber with identifier {}. The graphics client will now close.",
                                  client );

                    client.stop();
                }
            }
        };

        this.timer.schedule( updater, 0, GraphicsClient.STATUS_UPDATE_MILLISECONDS );
        this.timer.schedule( healthChecker, 0, GraphicsClient.HEALTH_CHECK_MILLISECONDS );
    }

    /**
     * @return the client status.
     */

    private SubscriberStatus getSubscriberStatus()
    {
        return this.getGraphicsSubscriber()
                   .getSubscriberStatus();
    }

    /**
     * Closes the executors that serve this client.
     */

    private void closeExecutors()
    {
        this.getGraphicsExecutor()
            .shutdown();

        try
        {
            boolean terminated = this.getGraphicsExecutor()
                                     .awaitTermination( 5, TimeUnit.SECONDS );

            if ( !terminated )
            {
                List<Runnable> tasks = this.getGraphicsExecutor()
                                           .shutdownNow();

                if ( !tasks.isEmpty() && LOGGER.isInfoEnabled() )
                {
                    LOGGER.info( "Abandoned {} tasks from {}",
                                 tasks.size(),
                                 this.getGraphicsExecutor() );
                }
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while shutting down the graphics executor service {} in WRES Graphics "
                         + "Client {}.",
                         this.getGraphicsExecutor(),
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
            LOGGER.warn( "Failed to close all resources used by WRES Graphics Client {}.", this );

            Thread.currentThread().interrupt();
        }
    }

    /**
     * @return the graphics subscriber.
     */

    private EvaluationSubscriber getGraphicsSubscriber()
    {
        return this.graphicsSubscriber;
    }

    /**
     * Awaits completion.
     * @throws InterruptedException if the server is interrupted.
     */

    private void await() throws InterruptedException
    {
        this.latch.await();
    }

    /**
     * @return the subscriber identifier.
     */

    private String getClientId()
    {
        return this.getGraphicsSubscriber()
                   .getClientId();
    }

    /**
     * @return the executor to do graphics writing work.
     */

    private ExecutorService getGraphicsExecutor()
    {
        return this.graphicsWorker;
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
     */

    private GraphicsClient( BrokerConnectionFactory broker )
    {
        LOGGER.info( "Creating WRES Graphics Client..." );

        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an internal error in WRES Graphics Client " + this + ".";
            LOGGER.error( message, b );
            this.stop();
        };

        ThreadFactory graphicsFactory = new BasicThreadFactory.Builder().namingPattern( "Graphics Client Thread %d" )
                                                                        .uncaughtExceptionHandler( handler )
                                                                        .build();

        this.clientExecutor = Executors.newSingleThreadExecutor( graphicsFactory );
        this.timer = new Timer( true );
        this.latch = new CountDownLatch( 1 );
        this.graphicsWorker = this.createGraphicsExecutor();
        this.isClosed = new AtomicBoolean();

        // Client identifier = identifier of the one subscriber it composes
        String subscriberId = EvaluationEventUtilities.getId();

        // A factory that creates consumers on demand
        this.consumerFactory = new GraphicsConsumerFactory( subscriberId );

        try
        {
            this.graphicsSubscriber = EvaluationSubscriber.of( this.consumerFactory,
                                                               this.getGraphicsExecutor(),
                                                               broker,
                                                               false );
        }
        catch ( UnrecoverableSubscriberException e )
        {
            throw new GraphicsClientException( "While attempting to build a WRES Graphics Client, encountered an "
                                               + "error.",
                                               e );
        }

        LOGGER.info( "Finished creating WRES Graphics Client with subscriber identifier {}.", this );
    }

    /**
     * Creates an executor service.
     * @return the created service
     */

    private ExecutorService createGraphicsExecutor()
    {
        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an internal error in a WRES Graphics Worker of WRES Graphics Client "
                             + this
                             + ".";
            LOGGER.error( message, b );
            this.stop();
        };

        ThreadFactory graphicsFactory = new BasicThreadFactory.Builder().namingPattern( "Graphics Worker Thread %d" )
                                                                        .uncaughtExceptionHandler( handler )
                                                                        .build();

        BlockingQueue<Runnable> graphicsQueue = new ArrayBlockingQueue<>( GraphicsClient.MAXIMUM_THREAD_COUNT * 5 );

        ThreadPoolExecutor pool = new ThreadPoolExecutor( GraphicsClient.MAXIMUM_THREAD_COUNT,
                                                          GraphicsClient.MAXIMUM_THREAD_COUNT,
                                                          GraphicsClient.MAXIMUM_THREAD_COUNT,
                                                          TimeUnit.MILLISECONDS,
                                                          graphicsQueue,
                                                          graphicsFactory );

        // Punt to the main thread to slow progress when rejected
        pool.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );

        return pool;
    }

    static class GraphicsClientException extends RuntimeException
    {

        /**
         * Serial identifier.
         */
        private static final long serialVersionUID = -3496487018421078900L;

        /**
         * Builds a {@link GraphicsClientException} with the specified message.
         * 
         * @param message the message.
         * @param cause the cause of the exception
         */

        public GraphicsClientException( String message, Throwable cause )
        {
            super( message, cause );
        }

    }

}

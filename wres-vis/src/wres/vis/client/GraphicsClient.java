package wres.vis.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import wres.events.Evaluation;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.UnrecoverableSubscriberException;
import wres.events.subscribe.EvaluationSubscriber.SubscriberStatus;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.util.Strings;

/**
 * A long-running graphics client that encapsulates one graphics subscriber, which consumes statistics and writes them 
 * to graphics.
 * 
 * @author james.brown@hydrosolved.com
 */

class GraphicsClient implements Closeable
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
     * The frequency with which to publish a subscriber-alive message in ms.
     */

    private static final long NOTIFY_ALIVE_MILLISECONDS = 100_000;

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
     * Start the graphics server.
     * @param args the command line arguments
     * @throws IOException if the server failed to stop
     */

    public static void main( String[] args ) throws IOException
    {
        // Print version information
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        String processId = Strings.extractWord( processName, "\\d+(?=@)" );

        MDC.put( "pid", processId );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( GraphicsClient.VERSION.getDescription() );
            LOGGER.info( GraphicsClient.VERSION.getVerboseRuntimeDescription() );
        }

        // Create the server
        int exitCode = 0;

        try ( BrokerConnectionFactory broker = BrokerConnectionFactory.of();
              GraphicsClient graphics = GraphicsClient.of( broker ) )
        {
            Instant started = Instant.now();

            // Add a shutdown hook to respond gracefully to SIGINT signals
            // Given the try-with-resources, this may initiate a close twice, 
            // but a shutdown hook works in more circumstances than a try/finally.
            Runtime.getRuntime().addShutdownHook( new Thread( () -> {
                Instant ended = Instant.now();
                Duration duration = Duration.between( started, ended );

                graphics.stop();
                LOGGER.info( "WRES Graphics Client {} ran for '{}' and processed {} packets of statistics across {} "
                             + "evaluations.",
                             graphics.getSubscriberId(),
                             duration,
                             graphics.getSubscriberStatus().getStatisticsCount(),
                             graphics.getSubscriberStatus().getEvaluationCount() );
            } ) );

            graphics.start();

            // Await termination
            graphics.await();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            LOGGER.error( "Interrupted while waiting for a WRES Graphics Client." );

            exitCode = 1;
        }
        catch ( GraphicsClientException f )
        {
            LOGGER.error( "Encountered an internal error in a WRES Graphics Client, which will now shut down. {}",
                          f.getMessage() );

            exitCode = 1;
        }

        System.exit( exitCode );
    }

    /**
     * Creates a timer task, which tracks the status of the server.
     */

    private void run()
    {
        LOGGER.info( "WRES Graphics client {} is running.", this.getSubscriberId() );

        // The status is mutable and is updated by the subscriber
        SubscriberStatus status = this.getSubscriberStatus();
        EvaluationSubscriber subscriber = this.getGraphicsSubscriber();
        GraphicsClient client = this;

        // Create a timer task to log the server status
        TimerTask sweeper = new TimerTask()
        {
            @Override
            public void run()
            {
                // Sweep any complete evaluations
                subscriber.sweep();

                // Log status
                LOGGER.info( "{}", status );
            }
        };

        // Create a timer task to update any listening clients that the subscriber is alive in case of long-running 
        // writing tasks
        TimerTask updater = new TimerTask()
        {
            @Override
            public void run()
            {
                // I am still alive
                subscriber.notifyAlive();
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
                                  client.getSubscriberId() );

                    client.close();
                }
            }
        };

        this.timer.schedule( sweeper, 0, GraphicsClient.STATUS_UPDATE_MILLISECONDS );
        this.timer.schedule( updater, 0, GraphicsClient.NOTIFY_ALIVE_MILLISECONDS );
        this.timer.schedule( healthChecker, 0, GraphicsClient.HEALTH_CHECK_MILLISECONDS );
    }

    @Override
    public void close()
    {
        this.stop();
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
     * @return the client status.
     */

    private SubscriberStatus getSubscriberStatus()
    {
        return this.getGraphicsSubscriber()
                   .getSubscriberStatus();
    }

    /**
     * @return the graphics subscriber.
     */

    private EvaluationSubscriber getGraphicsSubscriber()
    {
        return this.graphicsSubscriber;
    }

    /**
     * Starts the server.
     */

    void start()
    {
        this.clientExecutor.submit( this::run );
    }

    /**
     * Stops the server.
     */

    private void stop()
    {
        // Not stopped already?
        if ( this.latch.getCount() != 0 )
        {
            this.timer.cancel();

            if ( Objects.nonNull( this.graphicsSubscriber ) )
            {
                this.graphicsSubscriber.close();
            }

            this.getGraphicsExecutor()
                .shutdown();

            try
            {
                this.getGraphicsExecutor()
                    .awaitTermination( 5, TimeUnit.SECONDS );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while shutting down the graphics executor service {} in WRES Graphics "
                             + "Client {}.",
                             this.getGraphicsExecutor(),
                             this.getSubscriberId() );

                Thread.currentThread().interrupt();
            }

            this.getClientExecutor()
                .shutdown();

            try
            {
                this.getClientExecutor().awaitTermination( 1, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                LOGGER.warn( "Failed to close all resources used by WRES Graphics Client {}.", this.getSubscriberId() );

                Thread.currentThread().interrupt();
            }

            this.latch.countDown();

            LOGGER.info( "WRES Graphics Client {} has stopped.", this.getSubscriberId() );
        }
    }

    /**
     * Stops the server.
     * @throws InterruptedException if the server is interrupted.
     */

    private void await() throws InterruptedException
    {
        this.latch.await();
    }

    /**
     * @return the subscriber identifier.
     */

    private String getSubscriberId()
    {
        return this.getGraphicsSubscriber()
                   .getSubscriberId();
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
            String message = "Encountered an internal error in WRES Graphics Client " + this.getSubscriberId() + ".";
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

        // Client identifier = identifier of the one subscriber it composes
        String subscriberId = Evaluation.getUniqueId();

        try
        {
            // A factory that creates consumers on demand
            ConsumerFactory consumerFactory = new GraphicsConsumerFactory( subscriberId );

            this.graphicsSubscriber = EvaluationSubscriber.of( consumerFactory,
                                                               this.getGraphicsExecutor(),
                                                               broker );
        }
        catch ( UnrecoverableSubscriberException e )
        {
            throw new GraphicsClientException( "While attempting to build a WRES Graphics Client, encountered an "
                                               + "error.",
                                               e );
        }

        LOGGER.info( "Finished creating WRES Graphics Client with subscriber identifier {}.", this.getSubscriberId() );
    }

    /**
     * Creates an executor service.
     * @return the created service
     */

    private ExecutorService createGraphicsExecutor()
    {
        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an internal error in a WRES Graphics Worker of WRES Graphics Client "
                             + this.getSubscriberId()
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

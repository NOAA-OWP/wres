package wres.vis.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.events.Evaluation;
import wres.eventsbroker.BrokerConnectionFactory;

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
     * Maximum number of threads for graphics writing.
     */

    private static final int MAXIMUM_THREAD_COUNT = 5;

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
     * Server status.
     */

    private final ClientStatus clientStatus;

    /**
     * Wait until stopped.
     */

    private final CountDownLatch latch;

    /**
     * The graphics subscriber;
     */

    private final GraphicsSubscriber graphicsSubscriber;

    /**
     * Start the graphics server.
     * @param args the command line arguments
     * @throws IOException if the server failed to stop
     */

    public static void main( String[] args ) throws IOException
    {
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
                             graphics.getClientStatus().getStatisticsCount(),
                             graphics.getClientStatus().getEvaluationCount() );
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
        ClientStatus status = this.getClientStatus();
        GraphicsSubscriber subscriber = this.getGraphicsSubscriber();

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

        this.timer.schedule( sweeper, 0, GraphicsClient.STATUS_UPDATE_MILLISECONDS );
        this.timer.schedule( updater, 0, GraphicsClient.NOTIFY_ALIVE_MILLISECONDS );
    }

    @Override
    public void close() throws IOException
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

    private ClientStatus getClientStatus()
    {
        return this.clientStatus;
    }

    /**
     * @return the graphics subscriber.
     */

    private GraphicsSubscriber getGraphicsSubscriber()
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

            this.getGraphicsExecutor().shutdown();

            try
            {
                this.getGraphicsExecutor().awaitTermination( 5, TimeUnit.SECONDS );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while shutting down the graphics executor service {} in WRES Graphics "
                             + "Client {}.",
                             this.getGraphicsExecutor(),
                             this.getSubscriberId() );

                Thread.currentThread().interrupt();
            }

            this.getClientExecutor().shutdown();

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
        this.clientStatus = new ClientStatus( subscriberId, this::stop );

        try
        {
            this.graphicsSubscriber = new GraphicsSubscriber( subscriberId,
                                                              this.getClientStatus(),
                                                              this.getGraphicsExecutor(),
                                                              broker );
        }
        catch ( NamingException | JMSException | RuntimeException e )
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

    /**
     * A mutable container that records the status of the server and the jobs completed so far. All status information 
     * is updated atomically.
     * 
     * @author james.brown@hydrosolved.com
     */

    static class ClientStatus
    {

        /** The client identifier.*/
        private final String clientId;

        /** The number of evaluations completed.*/
        private final AtomicInteger evaluationCount = new AtomicInteger();

        /** The number of statistics blobs completed.*/
        private final AtomicInteger statisticsCount = new AtomicInteger();

        /** The last evaluation started.*/
        private final AtomicReference<String> evaluationId = new AtomicReference<>();

        /** The last statistics message completed.*/
        private final AtomicReference<String> statisticsMessageId = new AtomicReference<>();

        /** The evaluations that failed.*/
        private final Set<String> evaluationFailed = ConcurrentHashMap.newKeySet();

        /** The evaluations that have completed.*/
        private final Set<String> evaluationComplete = ConcurrentHashMap.newKeySet();

        /** A stop action to perform when the client fails unrecoverably.**/
        private final Runnable stopAction;

        @Override
        public String toString()
        {
            String addSucceeded = "";
            String addFailed = "";
            String addComplete = "";

            if ( Objects.nonNull( this.evaluationId.get() ) && Objects.nonNull( this.statisticsMessageId.get() ) )
            {
                addSucceeded = " The most recent evaluation was "
                               + this.evaluationId.get()
                               + " and the most recent statistics were attached to message "
                               + this.statisticsMessageId.get()
                               + ".";
            }

            if ( !this.evaluationFailed.isEmpty() )
            {
                addFailed =
                        " Failed to consume one or more statistics messages for " + this.evaluationFailed.size()
                            + " evaluations. "
                            + "The failed evaluation are "
                            + this.evaluationFailed
                            + ".";
            }

            if ( !this.evaluationComplete.isEmpty() )
            {
                addComplete = " Completed " + this.evaluationComplete.size()
                              + " of the "
                              + this.evaluationCount.get()
                              + " evaluations that were started.";
            }

            return "Waiting for statistics. Until now, received "
                   + this.statisticsCount.get()
                   + " packets of statistics across "
                   + this.evaluationCount.get()
                   + " evaluations."
                   + addSucceeded
                   + addFailed
                   + addComplete;
        }

        /**
         * Increment the evaluation count and last evaluation identifier.
         * @param evaluationId the evaluation identifier
         */

        void registerEvaluationStarted( String evaluationId )
        {
            this.evaluationCount.incrementAndGet();
            this.evaluationId.set( evaluationId );
        }

        /**
         * Registers an evaluation completed.
         * @param evaluationId the evaluation identifier
         */

        void registerEvaluationCompleted( String evaluationId )
        {
            this.evaluationComplete.add( evaluationId );
        }

        /**
         * Increment the failed evaluation count and last failed evaluation identifier.
         * @param evaluationId the evaluation identifier
         */

        void registerFailedEvaluation( String evaluationId )
        {
            this.evaluationFailed.add( evaluationId );
        }

        /**
         * Increment the statistics count and last statistics message identifier.
         * @param messageId the identifier of the message that contained the statistics.
         */

        void registerStatistics( String messageId )
        {
            this.statisticsCount.incrementAndGet();
            this.statisticsMessageId.set( messageId );
        }

        /**
         * @return the evaluation count.
         */
        int getEvaluationCount()
        {
            return this.evaluationCount.get();
        }

        /**
         * @return the statistics count.
         */
        int getStatisticsCount()
        {
            return this.statisticsCount.get();
        }

        /**
         * Flags an unrecoverable failure in the graphics client.
         * @param exception the unrecoverable consumer exception that caused the failure
         */

        void markFailedUnrecoverably( UnrecoverableConsumerException exception )
        {
            String failure = "WRES Graphics Client " + clientId + " has failed unrecoverably and will now stop.";

            LOGGER.error( failure, exception );

            this.stopAction.run();
        }

        /**
         * Hidden constructor.
         * 
         * @param clientId the client identifier
         * @param stopAction a callback composing a stop action to perform when the client fails unrecoverably.
         */

        private ClientStatus( String clientId, Runnable stopAction )
        {
            Objects.requireNonNull( clientId );
            Objects.requireNonNull( stopAction );

            this.clientId = clientId;
            this.stopAction = stopAction;
        }
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

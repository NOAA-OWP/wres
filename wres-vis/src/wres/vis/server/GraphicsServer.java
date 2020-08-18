package wres.vis.server;

import java.io.Closeable;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A long-running server process that encapsulates one graphics subscriber that consumes statistics and writes them to 
 * graphics.
 * 
 * @author james.brown@hydrosolved.com
 */

public class GraphicsServer implements Runnable, Closeable
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsServer.class );

    /**
     * The frequency with which to log the status of the server in ms.
     */

    private static final long STATUS_UPDATE_MILLISECONDS = 10_000;

    /**
     * An executor to execute the graphics server.
     */

    private final ExecutorService server;

    /**
     * A timer task to print information about the status of the server.
     */

    private final Timer timer;

    /**
     * Server status.
     */

    private final ServerStatus serverStatus;

    /**
     * Wait until stopped.
     */

    private final CountDownLatch latch;

    /**
     * The graphics subscriber;
     */

    private final GraphicsSubscriber subscriber;

    /**
     * The unique identifier of the subscriber encapsulated by this server.
     */

    private final String subscriberId;

    /**
     * Start the graphics server.
     * @param args the command line arguments
     * @throws IOException if the server failed to stop
     */

    public static void main( String[] args ) throws IOException
    {
        // Create the server
        try ( GraphicsServer server = new GraphicsServer() )
        {
            Instant started = Instant.now();

            // Add a shutdown hook to respond gracefully to SIGINT signals
            Runtime.getRuntime().addShutdownHook( new Thread( () -> {
                Instant ended = Instant.now();
                Duration duration = Duration.between( started, ended );
                server.stop();
                LOGGER.info( "Stopping the WRES Graphics Server, which ran for '{}' and processed {} packets of "
                             + "statistics across {} evaluations.",
                             duration,
                             server.getServerStatus().getStatisticsCount(),
                             server.getServerStatus().getEvaluationCount() );
            } ) );

            server.start();

            // Await termination
            server.await();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            LOGGER.error( "Interrupted while waiting for the WRES Graphics Server." );
        }
        catch ( GraphicsServerException f )
        {
            LOGGER.error( "Encountered an internal error in the WRES Graphics Server, which will now shut down. {}",
                          f.getMessage() );
        }
    }

    /**
     * Creates a timer task, which tracks the status of the server.
     */

    @Override
    public void run()
    {
        // The status is mutable and is updated by the subscriber
        ServerStatus status = this.getServerStatus();

        // Create a timer task to log the server status
        TimerTask timed = new TimerTask()
        {
            @Override
            public void run()
            {
                LOGGER.info( "{}", status );
            }
        };

        this.timer.schedule( timed, 0, GraphicsServer.STATUS_UPDATE_MILLISECONDS );
    }

    @Override
    public void close() throws IOException
    {
        this.stop();
    }

    /**
     * @return the server status.
     */

    private ServerStatus getServerStatus()
    {
        return this.serverStatus;
    }

    /**
     * Starts the server.
     */

    private void start()
    {
        this.server.execute( this );
    }

    /**
     * Stops the server.
     */

    private void stop()
    {
        // Shutdown
        this.server.shutdown();

        try
        {
            this.server.awaitTermination( 1, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            LOGGER.error( "Failed to close all resources used by the WRES Graphics Server." );
        }

        this.timer.cancel();
        this.latch.countDown();

        if ( Objects.nonNull( subscriber ) )
        {
            this.subscriber.close();
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
     * Do not construct.
     */

    private GraphicsServer()
    {
        LOGGER.info( "Creating a new WRES Graphics Server..." );

        // This identifier is registered with the wres-core as a graphics subscriber. For now, it is manually declared
        // here, but it needs to come from configuration (each one starting a new server).
        this.subscriberId = "4mOgkGkse3gWIGKuIhzVnl5ZPCM";

        UncaughtExceptionHandler handler = ( a, b ) -> {
            LOGGER.error( "Encountered an internal error in a WRES Graphics Server: {}", b.getMessage() );
            this.stop();
        };

        ThreadFactory graphicsFactory = new BasicThreadFactory.Builder().namingPattern( "Graphics Server Thread %d" )
                                                                        .uncaughtExceptionHandler( handler )
                                                                        .build();

        this.server = Executors.newSingleThreadExecutor( graphicsFactory );
        this.timer = new Timer();
        this.serverStatus = new ServerStatus();
        this.latch = new CountDownLatch( 1 );

        try
        {
            this.subscriber = new GraphicsSubscriber( subscriberId, this.serverStatus );
        }
        catch ( NamingException constructionException )
        {
            this.stop();

            throw new GraphicsServerException( "While attempting to build graphics server "
                                               + subscriberId
                                               + ", encountered an error.",
                                               constructionException );
        }
        catch ( JMSException messagingException )
        {
            this.stop();

            throw new GraphicsServerException( "While attempting to subscribe to statistics, encountered an error in "
                                               + "graphics server "
                                               + subscriberId
                                               + ".",
                                               messagingException );
        }
        // Other internal exceptions to propagate
        catch ( RuntimeException internalException )
        {
            this.stop();

            throw new GraphicsServerException( "While attempting to subscribe to statistics, encountered an internal "
                                               + "error in graphics server "
                                               + subscriberId
                                               + ".",
                                               internalException );
        }

        LOGGER.info( "Finished creating a new WRES Graphics Server." );
    }

    /**
     * A mutable container that records the status of the server and the jobs completed so far. All status information 
     * is updated atomically.
     * 
     * @author james.brown@hydrosolved.com
     */

    static class ServerStatus
    {

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

        @Override
        public String toString()
        {
            String addSucceeded = "";
            String addFailed = "";

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

            return "Waiting for statistics. Until now, received "
                   + this.statisticsCount.get()
                   + " packets of statistics across "
                   + this.evaluationCount.get()
                   + " evaluations."
                   + addSucceeded
                   + addFailed;
        }

        /**
         * Increment the evaluation count and last evaluation identifier.
         * @param evaluationId the evaluation identifier
         */

        void registerEvaluation( String evaluationId )
        {
            this.evaluationCount.incrementAndGet();
            this.evaluationId.set( evaluationId );
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
         * Increment the evaluation count and last evaluation identifier.
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
    }

    static class GraphicsServerException extends RuntimeException
    {

        /**
         * Serial identifier.
         */
        private static final long serialVersionUID = -3496487018421078900L;

        /**
         * Builds a {@link GraphicsServerException} with the specified message.
         * 
         * @param message the message.
         * @param cause the cause of the exception
         */

        public GraphicsServerException( String message, Throwable cause )
        {
            super( message, cause );
        }

    }

}

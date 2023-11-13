package wres.pipeline;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.events.EvaluationMessager;
import wres.events.subscribe.EvaluationSubscriber;
import wres.io.database.Database;

/**
 * A callback that cancels a running evaluation promptly.
 *
 * @author James Brown
 */
public class Canceller
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Canceller.class );

    /** Cancellation status. */
    private final AtomicBoolean cancelled = new AtomicBoolean();

    /** The evaluation identifier. */
    private String evaluationId = "unknown";

    /** An evaluation messager that should be stopped when available, notifying clients gracefully. */
    private EvaluationMessager messager;

    /** An internal formats subscriber that should be cancelled promptly. */
    private EvaluationSubscriber internalSubscriber;

    /** Database activities that should be cancelled promptly. */
    private Database database;

    /** Executors that should be cancelled promptly. */
    private Evaluator.Executors executors;

    /**
     * Creates an instance.
     * @return a canceller
     */

    public static Canceller of()
    {
        return new Canceller();
    }

    /**
     * Kill the executor service passed in even if there are remaining tasks.
     *
     * @param executor the executor to shut down
     */
    public static void closeGracefully( ExecutorService executor )
    {
        if ( Objects.nonNull( executor ) )
        {
            // Shutdown
            executor.shutdown();

            try
            {
                // Await termination after shutdown
                boolean died = executor.awaitTermination( 5, TimeUnit.SECONDS );

                if ( !died )
                {
                    List<Runnable> tasks = executor.shutdownNow();

                    if ( !tasks.isEmpty() && LOGGER.isInfoEnabled() )
                    {
                        LOGGER.info( "Abandoned {} tasks from {}",
                                     tasks.size(),
                                     executor );
                    }
                }
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while shutting down {}", executor, ie );
                Thread.currentThread()
                      .interrupt();
            }
        }
    }

    /**
     * If not already cancelled, cancels the running evaluation.
     */
    public void cancel()
    {
        if ( !this.cancelled.getAndSet( true ) )
        {
            LOGGER.warn( "Cancelling evaluation {}, which may produce errors...", this.evaluationId );

            this.cancelMessager();
            this.cancelSubscriber();
            this.cancelDatabaseActivities();
            this.cancelEvaluationExecutors();

            LOGGER.info( "Cancelled evaluation {}.", this.evaluationId );
        }
    }

    /**
     * @return whether the evaluation was cancelled
     */

    public boolean cancelled()
    {
        return this.cancelled.get();
    }

    /**
     * Sets the evaluation messager to cancel.
     * @param messager the evaluation messager, not null
     * @throws NullPointerException if the messager is null
     */
    void setEvaluationMessager( EvaluationMessager messager )
    {
        Objects.requireNonNull( messager );

        LOGGER.debug( "Set the evaluation messager to cancel for evaluation {}.", this.evaluationId );

        this.messager = messager;

        // Cancel if cancellation request already received
        if ( this.cancelled() )
        {
            this.cancelMessager();
        }
    }

    /**
     * Sets the internal formats subscriber to cancel.
     * @param internalSubscriber the internal formats subscriber, not null
     * @throws NullPointerException if the subscriber is null
     */
    void setInternalFormatsSubscriber( EvaluationSubscriber internalSubscriber )
    {
        Objects.requireNonNull( internalSubscriber );

        LOGGER.debug( "Set the internal formats subscriber to cancel for evaluation {}.", this.evaluationId );

        this.internalSubscriber = internalSubscriber;

        // Cancel if cancellation request already received
        if ( this.cancelled() )
        {
            this.cancelSubscriber();
        }
    }

    /**
     * Sets the evaluation executors to cancel
     * @param executors the executors
     */
    void setEvaluationExecutors( Evaluator.Executors executors )
    {
        this.executors = executors;

        // Cancel if cancellation request already received
        if ( this.cancelled() )
        {
            this.cancelEvaluationExecutors();
        }
    }

    /**
     * Sets the database to cancel.
     * @param database the database
     */
    void setDatabase( Database database )
    {
        this.database = database;

        // Cancel if cancellation request already received
        if ( this.cancelled() )
        {
            this.cancelDatabaseActivities();
        }
    }

    /**
     * Sets the evaluation identifier.
     *
     * @param evaluationId the evaluation identifier, not null
     * @throws NullPointerException if the identifier is null
     */

    void setEvaluationId( String evaluationId )
    {
        Objects.requireNonNull( evaluationId );

        this.evaluationId = evaluationId;
    }

    /**
     * Cancels the evaluation subscriber.
     */

    private void cancelSubscriber()
    {
        if ( Objects.nonNull( this.internalSubscriber ) )
        {
            LOGGER.warn( "Cancelling the internal formats subscriber for evaluation {}, which may produce errors...",
                         this.evaluationId );

            try
            {
                this.internalSubscriber.close();
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Encountered an error while attempting to close an internal formats subscriber.", e );
            }
        }
    }

    /**
     * Cancels the evaluation messager.
     */

    private void cancelMessager()
    {
        if ( Objects.nonNull( this.messager ) )
        {
            LOGGER.warn( "Cancelling the evaluation messager for evaluation {}, which may produce errors...",
                         this.evaluationId );

            CancellationException c = new CancellationException( "Received a request to cancel evaluation "
                                                                 + this.evaluationId
                                                                 + "." );

            this.messager.stop( c );
        }
    }

    /**
     * Cancels the evaluation executors.
     */

    private void cancelEvaluationExecutors()
    {
        if ( Objects.nonNull( this.executors ) )
        {
            LOGGER.warn( "Cancelling the evaluation executors for evaluation {}, which may produce errors...",
                         this.evaluationId );

            Canceller.closeGracefully( this.executors.readingExecutor() );
            Canceller.closeGracefully( this.executors.ingestExecutor() );
            Canceller.closeGracefully( this.executors.productExecutor() );
            Canceller.closeGracefully( this.executors.metricExecutor() );
            Canceller.closeGracefully( this.executors.slicingExecutor() );
            Canceller.closeGracefully( this.executors.poolExecutor() );
            Canceller.closeGracefully( this.executors.samplingUncertaintyExecutor() );
        }
    }

    /**
     * Cancels database activities.
     */

    private void cancelDatabaseActivities()
    {
        if( Objects.nonNull( this.database ) )
        {
            LOGGER.warn( "Cancelling database activities for evaluation {}, which may produce errors...",
                         this.evaluationId );

            this.database.shutdown();
        }
    }

    /**
     * Hidden constructor.
     */
    private Canceller()
    {
    }
}
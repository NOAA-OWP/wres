package wres.pipeline;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.events.EvaluationMessager;
import wres.events.subscribe.EvaluationSubscriber;

/**
 * A callback that cancels a running evaluation, clearing up gracefully and then throwing a
 * {@link CancellationException} to stop further activity.
 *
 * @author James Brown
 */
public class Canceller
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Canceller.class );

    /** Cancellation status. */
    private final AtomicBoolean isCancelled = new AtomicBoolean();

    /** The evaluation identifier. */
    private String evaluationId = "";

    /** An evaluation messager that should be stopped when available, notifying clients gracefully. */
    private EvaluationMessager messager;

    /** An internal formats subscriber that should be cancelled promptly. */
    private EvaluationSubscriber internalSubscriber;

    /**
     * Creates an instance.
     * @return a canceller
     */

    public static Canceller of()
    {
        return new Canceller();
    }

    /**
     * If not already cancelled, cancels the running evaluation and throws a {@link CancellationException}.
     * @throws CancellationException always
     */
    public void cancel()
    {
        if(! this.isCancelled.getAndSet( true ) )
        {
            LOGGER.warn( "Cancelling evaluation {}, which may produce errors...", this.evaluationId );

            this.cancelMessager();
            this.cancelSubscriber();

            LOGGER.info( "Cancelled evaluation {}.", this.evaluationId );

            throw new CancellationException( "Received a request to cancel evaluation "
                                             + this.evaluationId
                                             + "." );
        }
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
        if( this.isCancelled.get() )
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
        if( this.isCancelled.get() )
        {
            this.cancelSubscriber();
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
     * Hidden constructor.
     */
    private Canceller()
    {
    }
}
package wres.events;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * Implements producer flow control to prevent a broker becoming overwhelmed with messages when producers are much
 * faster than consumers. Flow control is managed at the level of a message group. 
 * 
 * TODO: this implementation should be replaced with broker-managed flow control, rather than application-managed flow
 * control.
 * 
 * @author james.brown@hydrosolved.com
 */

class ProducerFlowController
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProducerFlowController.class );

    /**
     * The evaluation whose production should be controlled.
     */

    private final Evaluation evaluation;

    /**
     * The consumer identifiers to track. When every consumer has positive credit, flow control can be turned off. A
     * consumer gains credit when it reports {@link CompletionStatus#GROUP_CONSUMPTION_COMPLETE}. It loses credit after
     * flow control has been turned off.
     */

    private final Map<String, AtomicInteger> consumerCredit;

    /**
     * Lock that protects the {@link #consumerCredit}.
     */

    private final ReentrantLock lock;

    /**
     * Build an instance.
     * @param evaluation the evaluation whose flow should be controlled
     * @param consumers the consumer identifiers to track
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the set of consumers is empty
     */
    ProducerFlowController( Evaluation evaluation, Set<String> consumers )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( consumers );

        if ( consumers.isEmpty() )
        {
            throw new IllegalArgumentException( "While building a producer flow controller for evaluation "
                                                + evaluation.getEvaluationId()
                                                + ", found an empty set of consumers to track, which is not allowed." );
        }

        this.evaluation = evaluation;
        this.consumerCredit = new HashMap<>();

        for ( String next : consumers )
        {
            this.consumerCredit.put( next, new AtomicInteger() );
        }

        this.lock = new ReentrantLock();
    }

    /**
     * Starts producer flow control.
     */

    void start()
    {
        LOGGER.debug( "Engaging producer flow control for evaluation {} until all consumers report consumption "
                      + "complete for one message group.",
                      this.evaluation.getEvaluationId() );

        this.evaluation.startFlowControl();
    }

    /**
     * Attempts to stop producer flow control. Flow control will stop if sufficient credits are available. Sufficient
     * credits are available if all consumers have reported {@link CompletionStatus#GROUP_CONSUMPTION_COMPLETE} at 
     * least once and these credits have not been used.
     * 
     * @param consumerId the consumer that reports {@link CompletionStatus#GROUP_CONSUMPTION_COMPLETE}
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the set of consumers is empty
     */

    void stop( String consumerId )
    {
        Objects.requireNonNull( consumerId );

        if ( !this.consumerCredit.containsKey( consumerId ) )
        {
            throw new IllegalArgumentException( "While attempting to stop producer flow control for evaluation "
                                                + this.evaluation.getEvaluationId()
                                                + ", found an unrecognized consumer identifier '"
                                                + consumerId
                                                + "', which is not allowed. The allowed identifiers are: "
                                                + this.consumerCredit.keySet() );
        }

        // Lock the map to add and check the credit
        try
        {
            this.lock.lock();

            // Add the credit
            this.consumerCredit.get( consumerId )
                               .incrementAndGet();

            // Check for credit and stop flow control if enough is there
            boolean hasCredit = this.consumerCredit.values()
                                                   .stream()
                                                   .allMatch( next -> next.get() > 0 );

            // Credit available, so decrement it and then stop flow control
            if ( hasCredit )
            {
                for ( AtomicInteger next : this.consumerCredit.values() )
                {
                    next.decrementAndGet();
                }

                this.evaluation.stopFlowControl();

                LOGGER.debug( "Disengaging producer flow control for evaluation {}.",
                              this.evaluation.getEvaluationId() );
            }
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Adds a consumer against which to control flow.
     * 
     * @param consumerId the consumer identifier
     */

    void addConsumer( String consumerId )
    {
        Objects.requireNonNull( consumerId );

        try
        {
            this.lock.lock();

            this.consumerCredit.put( consumerId, new AtomicInteger() );
        }
        finally
        {
            this.lock.unlock();
        }
    }

}

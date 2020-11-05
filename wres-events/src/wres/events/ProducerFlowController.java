package wres.events;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * Implements producer flow control to prevent a broker becoming overwhelmed with messages when producers are much
 * faster than consumers. Flow control is managed at the level of a message group. Subscribers are registered for flow
 * control once they have been negotiated.
 * 
 * TODO: this implementation should be replaced with broker-managed flow control, rather than application-managed flow
 * control.
 * 
 * @author james.brown@hydrosolved.com
 */
@ThreadSafe
class ProducerFlowController
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProducerFlowController.class );

    /**
     * The evaluation whose production should be controlled.
     */

    private final Evaluation evaluation;

    /**
     * The subscriber identifiers to track. When every subscriber has positive credit, flow control can be turned off. A
     * subscriber gains credit when it reports {@link CompletionStatus#GROUP_CONSUMPTION_COMPLETE}. It loses credit 
     * once the flow control has been turned off, i.e., turning off flow control represents the spending of credit.
     */

    @GuardedBy( "subscriberCreditLock" )
    private final Map<String, AtomicInteger> subscriberCredit;

    /**
     * Lock that protects the {@link #subscriberCredit}.
     */

    private final ReentrantLock subscriberCreditLock;

    /**
     * Build an instance.
     * @param evaluation the evaluation whose flow should be controlled
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the set of consumers is empty
     */
    ProducerFlowController( Evaluation evaluation )
    {
        Objects.requireNonNull( evaluation );

        this.evaluation = evaluation;
        this.subscriberCredit = new HashMap<>();
        this.subscriberCreditLock = new ReentrantLock();
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

        if ( !this.subscriberCredit.containsKey( consumerId ) )
        {
            throw new IllegalArgumentException( "While attempting to stop producer flow control for evaluation "
                                                + this.evaluation.getEvaluationId()
                                                + ", found an unrecognized consumer identifier '"
                                                + consumerId
                                                + "', which is not allowed. The allowed identifiers are: "
                                                + this.subscriberCredit.keySet() );
        }

        // Lock the map to add and check the credit
        try
        {
            this.subscriberCreditLock.lock();

            // Add the credit
            this.subscriberCredit.get( consumerId )
                                 .incrementAndGet();

            // Check for credit and stop flow control if enough is there
            boolean hasCredit = this.subscriberCredit.values()
                                                     .stream()
                                                     .allMatch( next -> next.get() > 0 );

            // Credit available, so decrement it and then stop flow control
            if ( hasCredit )
            {
                for ( AtomicInteger next : this.subscriberCredit.values() )
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
            this.subscriberCreditLock.unlock();
        }
    }

    /**
     * Adds a consumer against which to control flow.
     * 
     * @param subscriberId the consumer identifier
     */

    void addSubscriber( String subscriberId )
    {
        Objects.requireNonNull( subscriberId );

        try
        {
            this.subscriberCreditLock.lock();

            this.subscriberCredit.put( subscriberId, new AtomicInteger() );
        }
        finally
        {
            this.subscriberCreditLock.unlock();
        }
    }

}

package wres.events;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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
     * A lock to control the flow of producers and thereby avoid overwhelming the broker when consumers a much slower
     * than producers (a.k.a. producer flow control). TODO: delegate this to the broker when adopting a broker than 
     * supports flow control for topic exchanges via JMS (Qpid does not).
     */

    private final ReentrantLock flowControlLock;

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
        this.flowControlLock = new ReentrantLock();
    }

    /**
     * Starts producer flow control when consumption lags behind production.
     */

    void start()
    {
        LOGGER.debug( "Engaging producer flow control for evaluation {} until all consumers report consumption "
                + "complete for one message group.",
                this.evaluation.getEvaluationId() );
        
        try
        {
            // See #86076-9. The goal is to replace this locking with broker-managed flow control. Until then, it is
            // probably safer to control flow only if the current thread can acquire the lock within a short period.
            // This increases the risk of broker overflow if a subscriber dies, temporarily, but reduces the risk of
            // application deadlock.
            this.flowControlLock.tryLock( 10, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            LOGGER.warn( "Thread {} failed to acquire a flow control lock within a period of PT10S.",
                         Thread.currentThread() );
        }
    }

    /**
     * Stops producer flow control unconditionally. Should be stopped by the thread that started flow control.
     */

    void stop()
    {
        if ( this.flowControlLock.isHeldByCurrentThread() )
        {
            this.flowControlLock.unlock();
        }
        else
        {
            LOGGER.debug( "Cannot stop flow control with thread {} because it does not hold the flow control lock.",
                          Thread.currentThread() );
        }
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

                this.stop();

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

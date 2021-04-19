package wres.events.subscribe;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import net.jcip.annotations.ThreadSafe;

/**
 * A mutable container that records the status of the subscriber and the jobs completed so far. All status 
 * information is updated atomically.
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
public class SubscriberStatus
{

    /** The client identifier.*/
    private final String clientId;

    /** The number of evaluations started.*/
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

    /** Is true if the subscriber has failed.*/
    private final AtomicBoolean isFailed = new AtomicBoolean();

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
            addComplete = " Evaluation subscriber "
                          + this.clientId
                          + " completed "
                          + this.evaluationComplete.size()
                          + " of the "
                          + this.evaluationCount.get()
                          + " evaluations that were started.";
        }

        return "Evaluation subscriber "
               + this.clientId
               + " is waiting for work. Until now, received "
               + this.statisticsCount.get()
               + " packets of statistics across "
               + this.evaluationCount.get()
               + " evaluations."
               + addSucceeded
               + addFailed
               + addComplete;
    }

    /**
     * @return the evaluation count.
     */
    public int getEvaluationCount()
    {
        return this.evaluationCount.get();
    }

    /**
     * @return the evaluation failed count.
     */

    public int getEvaluationFailedCount()
    {
        return this.evaluationFailed.size();
    }

    /**
     * @return the statistics count.
     */
    public int getStatisticsCount()
    {
        return this.statisticsCount.get();
    }

    /**
     * Flags an unrecoverable failure in the subscriber.
     */

    public void markFailedUnrecoverably()
    {
        this.isFailed.set( true );
    }

    /**
     * Returns the failure state of the subscriber.
     * @return true if the subscriber has failed, otherwise false
     */

    public boolean isFailed()
    {
        return this.isFailed.get();
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

    void registerEvaluationFailed( String evaluationId )
    {
        this.evaluationFailed.add( evaluationId );
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
     * Increment the statistics count and last statistics message identifier.
     * @param messageId the identifier of the message that contained the statistics.
     */

    void registerStatistics( String messageId )
    {
        this.statisticsCount.incrementAndGet();
        this.statisticsMessageId.set( messageId );
    }

    /**
     * Hidden constructor.
     * 
     * @param clientId the client identifier
     */

    SubscriberStatus( String clientId )
    {
        Objects.requireNonNull( clientId );

        this.clientId = clientId;
    }
}
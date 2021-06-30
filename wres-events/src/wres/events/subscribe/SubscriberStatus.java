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

    /** The most evaluations underway at any one time.*/
    private final AtomicInteger maxUnderwayCount = new AtomicInteger();

    /** The last evaluation started.*/
    private final AtomicReference<String> evaluationId = new AtomicReference<>();

    /** The last statistics message completed.*/
    private final AtomicReference<String> statisticsMessageId = new AtomicReference<>();

    /** The evaluations that failed.*/
    private final Set<String> failed = ConcurrentHashMap.newKeySet();

    /** The evaluations that have completed.*/
    private final Set<String> complete = ConcurrentHashMap.newKeySet();

    /** The evaluations in progress.*/
    private final Set<String> underway = ConcurrentHashMap.newKeySet();

    /** Is true if the subscriber has failed.*/
    private final AtomicBoolean isFailed = new AtomicBoolean();

    @Override
    public String toString()
    {
        String addSucceeded = "";
        String addFailed = "";
        String addComplete = "";
        String addUnderway = "";
        String mostUnderway = "";

        if ( Objects.nonNull( this.evaluationId.get() ) && Objects.nonNull( this.statisticsMessageId.get() ) )
        {
            addSucceeded = " The most recent evaluation was "
                           + this.evaluationId.get()
                           + " and the most recent statistics were attached to message "
                           + this.statisticsMessageId.get()
                           + ".";
        }

        if ( !this.failed.isEmpty() )
        {
            addFailed =
                    " Failed to consume one or more statistics messages for " + this.failed.size()
                        + " evaluations. "
                        + "The failed evaluation are "
                        + this.failed
                        + ".";
        }

        if ( !this.complete.isEmpty() )
        {
            addComplete = " Evaluation subscriber "
                          + this.clientId
                          + " completed "
                          + this.complete.size()
                          + " of the "
                          + this.evaluationCount.get()
                          + " evaluations that were started.";
        }

        if ( !this.failed.isEmpty() || !this.complete.isEmpty() )
        {
            mostUnderway = " The most evaluations underway at the same time was " + this.maxUnderwayCount.get() + ".";
        }

        if ( !this.underway.isEmpty() )
        {
            addUnderway = " The " + this.underway.size() + " evaluations underway are " + this.underway + ".";
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
               + addComplete
               + addUnderway
               + mostUnderway;
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
        return this.failed.size();
    }

    /**
     * @return the number of evaluations completed.
     */

    public int getEvaluationCompletedCount()
    {
        return this.complete.size();
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
        this.complete.add( evaluationId );
        this.underway.remove( evaluationId );
    }

    /**
     * Increment the failed evaluation count and last failed evaluation identifier.
     * @param evaluationId the evaluation identifier
     */

    void registerEvaluationFailed( String evaluationId )
    {
        this.failed.add( evaluationId );
        this.underway.remove( evaluationId );
    }

    /**
     * Increment the evaluation count and last evaluation identifier.
     * @param evaluationId the evaluation identifier
     */

    void registerEvaluationStarted( String evaluationId )
    {
        this.evaluationCount.incrementAndGet();
        this.evaluationId.set( evaluationId );
        this.underway.add( evaluationId );

        // Update the maximum underway
        int maxCount = Math.max( this.maxUnderwayCount.get(), this.underway.size() );
        this.maxUnderwayCount.set( maxCount );
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
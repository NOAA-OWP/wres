package wres.events;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * <p>Used to track the completion state of an evaluation and to bind a latch to the reported completion state in order 
 * to await completion on closing an evaluation. Any client that listens for evaluation messages will need to establish 
 * when an evaluation has completed. The hooks for this are provided in the AMQP messaging, notably in the 
 * {@link EvaluationStatus} messages that are reported on completion of a message group or an entire evaluation. This
 * class uses those hooks to establish an implementation for the core-wres.
 * 
 * <p>This class is an implementation detail of the core-wres client that might be repeated in a similar (or different) 
 * way across other client consumers. This particular implementation is complicated by the need to track multiple 
 * consumers, as well as consumers that expect groups of messages. Clients that do "one thing", such as serialize 
 * pools of statistics to netcdf, should be much simpler.
 * 
 * <p>The expected pattern is one instance of a {@link CompletionTracker} per evaluation.
 * 
 * <p>Uses a special latch to wait for completion. As messages are received, a timeout is reset. When the timeout is
 * reached, the evaluation effectively expires. This is necessary to avoid an evaluation hanging indefinitely when a 
 * message is lost between production and consumption and all other mitigations are exhausted. In practice, it is 
 * extremely lenient and tries to balance the cost of running indefinitely with the cost of failing, akin to timeouts
 * used elsewhere in the software. This timeout is relative, not absolute. If progress occurs, the timeout is reset.
 * 
 * @author james.brown@hydrosolved.com
 */

class CompletionTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger( CompletionTracker.class );

    /**
     * The number of evaluation consumers registered for this notifier.
     */

    private final int evaluationConsumerCount;

    /**
     * The number of statistics consumers registered for this notifier.
     */

    private final int statisticsConsumerCount;

    /**
     * The number of evaluation status consumers registered for this notifier.
     */

    private final int evaluationStatusConsumerCount;

    /**
     * Number of consumers for each message group. This is used to determine the expected number of consumptions 
     * across all groups.
     */

    private final int numberOfConsumersPerGroup;

    /**
     * The expected number of messages per message group.
     */

    private final Map<String, Integer> expectedMessagesPerGroup;

    /**
     * A latch for consumption outside groups.
     */

    private final SafeCountUpAndDownLatch latch;

    /**
     * A latch to await the receipt of all expected evaluation status messages for message groups. An ordinary 
     * {@link CountDownLatch} would work here, except for the expiry time.
     */

    private final SafeCountUpAndDownLatch groupLatch;

    /**
     * The duration to wait for safety when blocking until an evaluation is complete. If this period is exceeded with
     * no porgress whatsoever, then progress will halt with an exception. This duration restarts whenever any progress
     * is made (i.e., a new message is consumed).
     */
    private final int expiryDurationInMinutes;

    /**
     * Returns an instance.
     * 
     * @param evaluationConsumerCount the number of evaluation description consumers
     * @param statisticsConsumerCount the number of statistics consumers
     * @param evaluationStatusConsumerCount the number of evaluation status consumers
     * @param statisticsGroupConsumerCount the number of consumers for statistics groups
     * @throws IllegalArgumentException if any count is <= 0
     */

    static CompletionTracker of( int evaluationConsumerCount,
                                 int statisticsConsumerCount,
                                 int evaluationStatusConsumerCount,
                                 int statisticsGroupConsumerCount )
    {
        return new CompletionTracker( evaluationConsumerCount,
                                      statisticsConsumerCount,
                                      evaluationStatusConsumerCount,
                                      statisticsGroupConsumerCount );
    }

    /**
     * Registers a consumption event.
     */

    void register()
    {
        // Registration may happen before the expected number of messages is known
        // In that case, the latch will cache the registrations until the expected count is known
        this.latch.countDown();
    }

    /**
     * Registers the completion of a message group.
     * 
     * @param completionState a message indicating that a group has completed.
     * @param groupId the group identifier.
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the message is missing expected content
     */

    void registerGroupComplete( EvaluationStatus completionState, String groupId )
    {
        Objects.requireNonNull( completionState );
        Objects.requireNonNull( groupId );

        if ( completionState.getCompletionStatus() != CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
        {
            throw new IllegalArgumentException( "While registered the completion of group " + groupId
                                                + ", received an unexpected completion "
                                                + "status  "
                                                + completionState.getCompletionStatus()
                                                + ". Expected "
                                                + CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS );
        }

        if ( completionState.getMessageCount() == 0 )
        {
            throw new IllegalArgumentException( "The completion status message for group " + groupId
                                                + " is missing an expected count of messages." );
        }

        int groupCount = (int) completionState.getMessageCount();
        Integer key = this.expectedMessagesPerGroup.putIfAbsent( groupId, groupCount );

        // Register receipt of the group message
        if ( Objects.isNull( key ) )
        {
            this.groupLatch.countDown();

            LOGGER.debug( "Registered completion of group {}, which contained {} messages.",
                          groupId,
                          groupCount );
        }
    }

    /**
     * Awaits the completion of an evaluation signalled in the input message.
     * 
     * @param completionState a message indicating that the evaluation has completed.
     * @throws InterruptedException 
     * @throws NullPointerException if the message is null
     * @throws IllegalArgumentException if the message is missing expected content
     * @throws InterrupedException if the wait was interrupted
     */

    void await( EvaluationStatus completionState ) throws InterruptedException
    {
        Objects.requireNonNull( completionState );

        if ( completionState.getCompletionStatus() != CompletionStatus.COMPLETE_REPORTED_SUCCESS )
        {
            throw new IllegalArgumentException( "Can only register a latch against a completion status of "
                                                + CompletionStatus.COMPLETE_REPORTED_SUCCESS );
        }

        if ( completionState.getMessageCount() == 0 )
        {
            throw new IllegalArgumentException( "The completion status message is missing an expected count of "
                                                + "messages for statistics and evaluation descriptions." );
        }

        if ( completionState.getStatusMessageCount() == 0 )
        {
            throw new IllegalArgumentException( "The completion status message is missing an expected count of "
                                                + "evaluation status messages." );
        }

        if ( completionState.getGroupCount() == 0 && this.numberOfConsumersPerGroup > 0 )
        {
            throw new IllegalArgumentException( "The evaluation has "
                                                + this.numberOfConsumersPerGroup
                                                + " group subscriptions, but the completion status message for this "
                                                + " evaluation does not include the expected count of "
                                                + "message groups." );
        }

        // Wait for expected group messages first, because this informs the total number of consumptions expected
        // Wait for up to a fixed period, but only in the absence of any progress whatsoever.
        // If the latch counts down, the fixed period is reset.
        // This remains extremely lenient, i.e. not a single message consumed within the fixed period.
        this.groupLatch.addCount( completionState.getGroupCount() );
        this.groupLatch.waitFor( this.getExpiryDurationInMinutes(), TimeUnit.MINUTES );

        if ( this.groupLatch.getCount() != 0 )
        {
            throw new EvaluationEventException( "On waiting for an evaluation to complete, too much time elapsed with "
                                                + "no progress. No evaluation status messages for group completion "
                                                + "were consumed within a period of PT1H. Stopping the evaluation for "
                                                + "safety. Please contact a service administrator for assistance." );
        }

        // Report on groups complete if grouping consumers exist
        if ( this.numberOfConsumersPerGroup > 0 )
        {
            LOGGER.debug( "Completion notifier has received the expected number of groups. Notifier is {}", this );
        }

        // Sum the expected number of consumptions across all consumers
        int totalCount = this.evaluationConsumerCount * 1; // 1 evaluation description message (EDM) per evaluation
        totalCount += this.statisticsConsumerCount * ( completionState.getMessageCount() - 1 ); // -1 EDM
        totalCount += this.evaluationStatusConsumerCount * completionState.getStatusMessageCount();
        totalCount += this.getGroupConsumptionCount();

        // Now the expected count is known, so add it. 
        // Any existing stored registrations will also be subtracted at this point.
        this.latch.addCount( totalCount );

        LOGGER.debug( "Updated completion notifier {} Registered {} expected consumptions, {} completed consumptions "
                      + "and {} outstanding consumptions.",
                      this,
                      totalCount,
                      ( totalCount - this.latch.getCount() ),
                      this.latch.getCount() );

        // Now wait for all other messages for up to a fixed period, but only in the absence of any progress whatsoever.
        // If the latch counts down, the fixed period is reset.
        // This remains extremely lenient, i.e. not a single message consumed within the fixed period.
        this.latch.waitFor( this.getExpiryDurationInMinutes(), TimeUnit.MINUTES );

        if ( this.latch.getCount() != 0 )
        {
            throw new EvaluationEventException( "On waiting for an evaluation to complete, too much time elapsed with "
                                                + "no progress. No evaluation messages were consumed within a period "
                                                + "of PT1H. Stopping the evaluation for safety. Please contact a "
                                                + "service administrator for assistance." );
        }
    }

    /**
     * Returns the expected number of messages per group or null if no mapping exists.
     * 
     * @param groupId the group identifier
     * @return the expected number of messages per group
     * @throws NullPointerException if the input is null
     */

    Integer getExpectedMessagesPerGroup( String groupId )
    {
        Objects.requireNonNull( groupId );

        return this.expectedMessagesPerGroup.get( groupId );
    }

    /**
     * A string representation, reporting on the total number of messages registered.
     * 
     * @return a string representation
     */

    public String toString()
    {
        return super.toString() + ": "
               + this.latch.getCount()
               + " messages notified consumed across "
               + this.evaluationConsumerCount
               + " evaluation consumers, "
               + this.statisticsConsumerCount
               + " statistics consumers (not including consumers of message groups) and "
               + this.evaluationStatusConsumerCount
               + " evaluation status consumers.";
    }

    /**
     * Hidden constructor.
     * 
     * @param evaluationConsumerCount the number of evaluation description consumers
     * @param statisticsConsumerCount the number of statistics consumers
     * @param evaluationStatusConsumerCount the number of evaluation status consumers
     * @param statisticsGroupConsumerCount the number of consumers of grouped statistics messages
     * @throws IllegalArgumentException if any count is <= 0
     */

    private CompletionTracker( int evaluationConsumerCount,
                               int statisticsConsumerCount,
                               int evaluationStatusConsumerCount,
                               int statisticsGroupConsumerCount )
    {
        if ( evaluationConsumerCount <= 0 )
        {
            throw new IllegalArgumentException( "The evaluation consumer count must be >= 0. " );
        }

        if ( statisticsConsumerCount <= 0 && statisticsGroupConsumerCount <= 0  )
        {
            throw new IllegalArgumentException( "The statistics consumer count must be >= 0. " );
        }

        if ( evaluationStatusConsumerCount <= 0 )
        {
            throw new IllegalArgumentException( "The evaluation status consumer count must be >= 0. " );
        }

        this.evaluationConsumerCount = evaluationConsumerCount;
        this.statisticsConsumerCount = statisticsConsumerCount;
        this.evaluationStatusConsumerCount = evaluationStatusConsumerCount;
        this.expectedMessagesPerGroup = new ConcurrentHashMap<>();

        // Register the number of consumers per group
        this.numberOfConsumersPerGroup = statisticsGroupConsumerCount;

        // Set the latches
        this.groupLatch = new SafeCountUpAndDownLatch();
        // The expected number of messages is unknown until a status message is received
        this.latch = new SafeCountUpAndDownLatch();

        // If this duration elapses with no messages incremented, then completion fails for safety
        this.expiryDurationInMinutes = 60;
    }

    /**
     * Iterates all all group subscriptions and computes the sum product of the number of subscribers per group and the
     * expected number of messages per group, which is the expected number of grouped consumptions.
     * 
     * @return the expected number of grouped consumptions
     */

    private int getGroupConsumptionCount()
    {
        int total = 0;

        for ( Map.Entry<String, Integer> next : this.expectedMessagesPerGroup.entrySet() )
        {
            int messages = next.getValue();
            total += ( this.numberOfConsumersPerGroup * messages );
        }

        return total;
    }

    /**
     * @return the duration for safe waits until the evaluation effectively expires, exceptionally.
     */
    private int getExpiryDurationInMinutes()
    {
        return this.expiryDurationInMinutes;
    }

    /**
     * Similar to {@link CountDownLatch} but counts up as well as down and facilitates waiting for a fixed period that 
     * is reset after each mutation. Thus, if there is no progress within a fixed period, then evaluation may still 
     * complete. Not to be exposed, since this is an implementation detail of the present class.
     */

    private static class SafeCountUpAndDownLatch
    {

        private final Sync sync;
        private AtomicLong timestamp;
        private AtomicInteger countToSubtractOnNextAdd;

        /**
         * Constructs a {@link CountingLatch} initialized to zero.
         */

        private SafeCountUpAndDownLatch()
        {
            this.sync = new Sync();
            this.timestamp = new AtomicLong( System.nanoTime() );
            this.countToSubtractOnNextAdd = new AtomicInteger();
        }

        /**
         * Decrements the count of the latch, releasing all waiting threads if the count reaches zero. However, if the
         * count is already zero, it will allow for the accumulation of a negative count to be applied at the next
         * {@link #addCount(int)}. 
         *
         * @see CountDownLatch#countDown()
         */

        private void countDown()
        {
            if ( this.getCount() == 0 )
            {
                this.countToSubtractOnNextAdd.incrementAndGet();
            }
            else
            {
                this.sync.releaseShared( -1 );
                this.timestamp = new AtomicLong( System.nanoTime() );
            }
        }

        /**
         * Adds to the count, removing any registered count that occurred while the latch count was zero. 
         * 
         * @see CountDownLatch#countDown()
         * 
         * @param count the amount to add (may be negative).
         */

        private void addCount( final int count )
        {
            int increment = count - this.countToSubtractOnNextAdd.get();

            this.sync.releaseShared( increment );
            this.timestamp = new AtomicLong( System.nanoTime() );
        }

        /**
         * Returns the current count.
         *
         * @see CountDownLatch#getCount()
         */
        public int getCount()
        {
            return this.sync.getCount();
        }

        /**
         * Causes the current thread to wait until the latch has counted down to zero, unless the thread is interrupted, or
         * the specified waiting time elapses.
         *
         * @param timeout the timeout period
         * @param unit the time unit
         * @return true if acquired, false if timed out
         * @see CountDownLatch#await(long,TimeUnit)
         */

        private boolean await( final long timeout, final TimeUnit unit ) throws InterruptedException
        {
            return this.sync.tryAcquireSharedNanos( 1, unit.toNanos( timeout ) );
        }

        /**
         * Causes the current thread to wait for a fixed period relative to the last mutation.
         * 
         * @param timeout the timeout period
         * @param unit the time unit
         * @return true if acquired, false if timed out
         * @throws InterruptedException
         */

        private boolean waitFor( long timeout, TimeUnit unit ) throws InterruptedException
        {
            long start = this.timestamp.get();
            long difference = 0;
            for ( ;; )
            {
                boolean result = this.await( unit.toNanos( timeout ) - difference, TimeUnit.NANOSECONDS );
                if ( this.timestamp.get() == start )
                {
                    return result;
                }
                start = this.timestamp.get();
                difference = System.nanoTime() - start;
            }
        }

        /**
         * Synchronization control.
         * 
         * Uses the {@link AbstractQueuedSynchronizer} state to represent count.
         */

        private static final class Sync extends AbstractQueuedSynchronizer
        {
            private static final long serialVersionUID = -7639904478060101736L;

            private Sync()
            {
            }

            private Sync( int count )
            {
                this.setState( count );
            }

            private int getCount()
            {
                return this.getState();
            }

            @Override
            protected int tryAcquireShared( final int acquires )
            {
                return this.getState() == 0 ? 1 : -1;
            }

            @Override
            protected boolean tryReleaseShared( final int delta )
            {
                if ( delta == 0 )
                {
                    return false;
                }

                // Loop until count is zero
                for ( ;; )
                {
                    final int c = super.getState();
                    int nextc = c + delta;
                    if ( c <= 0 && nextc <= 0 )
                    {
                        return false;
                    }
                    if ( nextc < 0 )
                    {
                        nextc = 0;
                    }
                    if ( super.compareAndSetState( c, nextc ) )
                    {
                        return nextc == 0;
                    }
                }
            }
        }
    }


}

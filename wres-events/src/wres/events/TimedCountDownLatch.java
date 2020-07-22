package wres.events;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Similar to {@link CountDownLatch} but facilitates waiting for a fixed period that is reset after each mutation. 
 * Thus, if there is no progress within a fixed period, an evaluation may still complete.
 * 
 * @author james.brown@hydrosolved.com
 */

class TimedCountDownLatch
{

    private final Sync sync;
    private AtomicLong timestamp;

    /**
     * Constructs a {@link TimedCountDownLatch} initialized to the prescribed count.
     * 
     * @param count the count
     * @throws IllegalArgumentException if the count is less than one
     */

    TimedCountDownLatch( int count )
    {
        if ( count < 1 )
        {
            throw new IllegalArgumentException( "The count must be one or more." );
        }

        this.sync = new Sync( count );
        this.timestamp = new AtomicLong( System.nanoTime() );
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if the count reaches zero. However, if the
     * count is already zero, it will allow for the accumulation of a negative count to be applied at the next
     * {@link #addCount(int)}. 
     *
     * @see CountDownLatch#countDown()
     */

    void countDown()
    {
        this.sync.releaseShared( -1 );
        resetClock();
    }

    /**
     * Resets the timeout clock.
     */

    void resetClock()
    {
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
     * Returns the current timestamp.
     * 
     * @return the current timestamp
     */

    private long getTime()
    {
        return this.timestamp.get();
    }

    /**
     * Causes the current thread to wait for a fixed period relative to the last mutation.
     * 
     * @param timeout the timeout period
     * @param unit the time unit
     * @return true if acquired, false if timed out
     * @throws InterruptedException
     */

    boolean await( long timeout, TimeUnit unit ) throws InterruptedException
    {
        long start = this.getTime();
        long difference = 0;
        for ( ;; )
        {
            boolean result = this.waitFor( unit.toNanos( timeout ) - difference, TimeUnit.NANOSECONDS );
            if ( this.getTime() == start )
            {
                return result;
            }
            start = this.getTime();
            difference = System.nanoTime() - start;
        }
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

    private boolean waitFor( final long timeout, final TimeUnit unit ) throws InterruptedException
    {
        return this.sync.tryAcquireSharedNanos( 1, unit.toNanos( timeout ) );
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

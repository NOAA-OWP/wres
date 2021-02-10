package wres.events;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.CountDownLatch;

/**
 * Similar to {@link CountDownLatch} but facilitates waiting for a fixed period that is reset after each mutation. 
 * Thus, if there is no progress within a fixed period, an evaluation may still complete.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimedCountDownLatch
{

    private final Sync sync;
    private final AtomicLong timestamp;
    private int resetCount;
    private boolean timedOut = false;
    
    /**
     * Constructs a {@link TimedCountDownLatch} initialized to the prescribed count.
     * 
     * @param count the count
     */

    public TimedCountDownLatch( int count )
    {
        this.sync = new Sync( count );
        this.timestamp = new AtomicLong( System.nanoTime() );
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if the count reaches zero.
     *
     * @see CountDownLatch#countDown()
     */

    public void countDown()
    {
        this.sync.releaseShared( -1 );
    }

    /**
     * Resets the timeout clock.
     */

    public void resetClock()
    {
        this.timestamp.set( System.nanoTime() );
        this.resetCount++;
    }
    
    /**
     * @return {@code true} if the timer timed out, otherwise {@code false}.
     */

    public boolean timedOut()
    {
        return this.timedOut;
    }
    
    /**
     * Causes the current thread to wait for a fixed period relative to the last mutation.
     * 
     * @param timeout the timeout period
     * @param unit the time unit
     * @return true if acquired, false if timed out
     * @throws InterruptedException if the timer is interrupted
     */

    public boolean await( long timeout, TimeUnit unit ) throws InterruptedException
    {
        long start = this.getTime();
        long periodToWait = unit.toNanos( timeout );
        for ( ;; )
        {
            // Wait until the timeout occurs or the count reaches zero, whichever is sooner
            boolean acquired = this.waitFor( periodToWait, TimeUnit.NANOSECONDS );
            
            this.timedOut = !acquired;
            
            // If the count reached zero or the original timeout occurred, then return
            if ( acquired || start == this.getTime() )
            {
                return acquired;
            }
            
            // Subtract the time already waited from the period to wait
            periodToWait = periodToWait - ( System.nanoTime() - start );
        }
    }
    
    /**
     * @return the number of times the timeout was reset
     */
    
    int getResetCount()
    {
        return this.resetCount;
    }
    
    /**
     * Returns the current count.
     *
     * @see CountDownLatch#getCount()
     */
    int getCount()
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

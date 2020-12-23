package wres.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * Tests the {@link TimedCountDownLatch}.
 * 
 * @author james.brown@hydrosolved.com
 */

class TimedCountDownLatchTest
{

    @Test
    void testResetLatchWaitsForLonger() throws InterruptedException
    {
        TimedCountDownLatch latch = new TimedCountDownLatch( 1 );

        // Create a timer task to log the server status
        AtomicInteger scheduledExecutions = new AtomicInteger();
        TimerTask updater = new TimerTask()
        {
            @Override
            public void run()
            {
                if ( scheduledExecutions.incrementAndGet() > 3 )
                {
                    latch.countDown();
                }
                else
                {
                    latch.resetClock();
                }
            }
        };

        Timer timer = new Timer();
        timer.schedule( updater, 0, 1000 );

        // Await up to 2000ms, but with the latch reset on a frequency of 1000ms, so the timeout should not be reached 
        // and the latch should exit via countdown instead
        latch.await( 2000, TimeUnit.MILLISECONDS );

        // Latch exited via countdown
        assertEquals( 0, latch.getCount() );
    }

}

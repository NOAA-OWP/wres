package wres.control;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ProcessorHelperTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProcessorHelperTest.class );

    @Test
    public void testDoAllOrExceptionResultHasException()
    {
        DummyTask failingTask = new DummyTask( Duration.ofMillis( 1 ),
                                                      true );
        CompletableFuture<String> f = CompletableFuture.supplyAsync( failingTask::call );
        CompletableFuture<Object> result = ProcessorHelper.doAllOrException( List.of( f ) );
        Throwable cause = assertThrows( CompletionException.class, result::join ).getCause();
        assertTrue( cause instanceof DummyException );
    }


    /**
     * A custom exception to ensure the correct exception propagates.
     */

    private static final class DummyException extends RuntimeException
    {
        DummyException( String message )
        {
            super( message );
        }
    }

    /**
     * A task that sleeps for given duration, throws when mustFail is true.
     */

    private static final class DummyTask implements Callable<String>
    {
        private final Duration duration;
        private final boolean mustFail;

        DummyTask( Duration duration,
                   boolean mustFail )
        {
            this.duration = duration;
            this.mustFail = mustFail;
        }

        @Override
        public String call()
        {
            Instant start = Instant.now();

            try
            {
                LOGGER.debug( "I am sleeping for {}", duration );
                Thread.sleep( duration.toMillis() );
                LOGGER.debug( "I am done sleeping for {}", duration );
            }
            catch ( InterruptedException ie )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "Interrupted", ie );
            }

            Instant end = Instant.now();
            String result = "I took a little more than "
                            + Duration.between( start, end );

            if ( this.mustFail )
            {
                LOGGER.debug( "I am throwing an exception momentarily." );
                throw new DummyException( result );
            }

            LOGGER.debug( "I am not throwing an exception." );
            return result;
        }
    }
}

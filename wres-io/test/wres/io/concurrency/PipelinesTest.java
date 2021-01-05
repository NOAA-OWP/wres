package wres.io.concurrency;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PipelinesTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PipelinesTest.class );
    private static final String TASK_RESULT_STRING_PREFIX = "I took a little more than ";

    @Test
    public void testDoAllOrExceptionResultHasException()
    {
        DummyTask failingTask = new DummyTask( Duration.ofMillis( 1 ),
                                                      true );
        CompletableFuture<String> f = CompletableFuture.supplyAsync( failingTask::call );
        CompletableFuture<Object> result = Pipelines.doAllOrException( List.of( f ) );
        Throwable cause = assertThrows( CompletionException.class, result::join ).getCause();
        assertTrue( cause instanceof DummyException );
    }


    @Test
    public void testDoAllOrExceptionResultHasNoException()
            throws InterruptedException, ExecutionException
    {
        DummyTask succeedingTask = new DummyTask( Duration.ofMillis( 1 ),
                                                  false );
        CompletableFuture<String> f = CompletableFuture.supplyAsync( succeedingTask::call );
        Pipelines.doAllOrException( List.of( f ) )
                 .join();
        assertTrue( f.isDone() );
        assertTrue( f.get()
                     .startsWith( TASK_RESULT_STRING_PREFIX ) );
    }


    @Test
    public void testDoAllOrExceptionCompletesMultipleTasks()
            throws InterruptedException, ExecutionException
    {
        DummyTask taskOne = new DummyTask( Duration.ofMillis( 1 ),
                                           false );
        DummyTask taskTwo = new DummyTask( Duration.ofMillis( 1 ),
                                           false );
        CompletableFuture<String> f1 = CompletableFuture.supplyAsync( taskOne::call );
        CompletableFuture<String> f2 = CompletableFuture.supplyAsync( taskTwo::call );
        Pipelines.doAllOrException( List.of( f1, f2 ) )
                 .join();
        assertTrue( f1.isDone() );
        assertTrue( f2.isDone() );
        assertTrue( f1.get()
                      .startsWith( TASK_RESULT_STRING_PREFIX ) );
        assertTrue( f2.get()
                      .startsWith( TASK_RESULT_STRING_PREFIX ) );
    }


    /**
     * Verify that when the shorter task is after the first (longer) task, while
     * the longer task does not throw an exception, and the shorter task does
     * throw an exception, that the exception propagates up within the expected
     * timeframe of "between duration of shorter task and longer task."
     */
    @Test
    public void testDoAllOrExceptionThrowsShorterTaskException()
    {
        Duration longerDuration = Duration.ofMillis( 10_000 );
        Duration shorterDuration = Duration.ofMillis( 10 );
        DummyTask firstLongerSucceedingTask = new DummyTask( longerDuration,
                                                             false );
        DummyTask secondShorterFailingTask = new DummyTask( shorterDuration,
                                                            true );
        Instant start = Instant.now();
        CompletableFuture<String> f1 = CompletableFuture.supplyAsync( firstLongerSucceedingTask::call );
        CompletableFuture<String> f2 = CompletableFuture.supplyAsync( secondShorterFailingTask::call );
        CompletableFuture<Object> result = Pipelines.doAllOrException( List.of( f1, f2 ) );
        Throwable cause = assertThrows( CompletionException.class, result::join ).getCause();
        Instant end = Instant.now();
        assertTrue( cause instanceof DummyException );
        Duration executionDuration = Duration.between( start, end );
        assertTrue( executionDuration.toMillis() > shorterDuration.toMillis()
                    && executionDuration.toMillis() < longerDuration.toMillis(),
                    "Expected execution duration " + executionDuration
                    + " to be shorter than " + longerDuration
                    + " and longer than " + shorterDuration );
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
            String result = TASK_RESULT_STRING_PREFIX
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

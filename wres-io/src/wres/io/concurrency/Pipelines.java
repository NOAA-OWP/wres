package wres.io.concurrency;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Class with methods to help work with processing pipelines.
 *
 * Select method migrated from wres.control.ProcessorHelper 2021-01-05 for the
 * purpose of re-use in wres.io.Operations. The wres module uses wres-io jar but
 * not vice versa.
 * @author james.brown@hydrosolved.com
 * @author jesse.bickel@***REMOVED***
 */
public class Pipelines
{
    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     *
     * @param <T> the type of future
     * @param futures the futures to compose
     * @return the composed futures
     * @throws CompletionException if completing exceptionally 
     */

    public static <T> CompletableFuture<Object> doAllOrException( final List<CompletableFuture<T>> futures )
    {
        //Complete when all futures are completed
        final CompletableFuture<Void> allDone =
                CompletableFuture.allOf( futures.toArray( new CompletableFuture[futures.size()] ) );
        //Complete when any of the underlying futures completes exceptionally
        final CompletableFuture<T> oneExceptional = new CompletableFuture<>();
        //Link the two
        for ( final CompletableFuture<T> completableFuture : futures )
        {
            //When one completes exceptionally, propagate
            completableFuture.exceptionally( exception -> {
                oneExceptional.completeExceptionally( exception );
                return null;
            } );
        }
        //Either all done OR one completes exceptionally
        return CompletableFuture.anyOf( allDone, oneExceptional );
    }

    private Pipelines()
    {
        // Helper class with static methods therefore no construction allowed.
    }
}

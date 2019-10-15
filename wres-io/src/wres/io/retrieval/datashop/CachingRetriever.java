package wres.io.retrieval.datashop;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>Adds a wrapper to a {@link Supplier} that supplies a {@link Stream} of retrieved data. This allow the retrieved 
 * data to be cached locally for re-use. On the second and further calls, the cached data is returned. Use this
 * wrapper whenever a data source is re-used between pools.
 * 
 * <p><b>Implementation notes:</b>
 * 
 * <p>This implementation is thread-safe.
 * 
 * @author james.brown@hydrosolved.com
 * @param <T> the type of data to retrieve
 */

public class CachingRetriever<T> implements Supplier<Stream<T>>
{

    /**
     * Status of the cache. If <code>true</code>, the cache is available, <code>false</code> otherwise.
     */

    private boolean hasCache;

    /**
     * Lock object.
     */

    private final Object lock;

    /**
     * Retriever.
     */

    private final Supplier<Stream<T>> retriever;

    /**
     * Cache of data.
     */

    private List<T> cache;

    /**
     * Provides an instance.
     * 
     * @param <T> the type of data to retrieve
     * @param retriever the retriever
     * @return the supplier
     */

    public static <T> CachingRetriever<T> of( Supplier<Stream<T>> retriever )
    {
        return new CachingRetriever<>( retriever );
    }

    @Override
    public Stream<T> get()
    {
        // Has cache?
        if ( !this.hasCache )
        {
            // Lock, in order to prepare cache
            synchronized ( this.lock )
            {
                // Check again, in case multiple threads 
                // found false on first check before sync
                if ( !this.hasCache )
                {
                    this.cache = this.getRetriever()
                                     .get()
                                     .collect( Collectors.toUnmodifiableList() );
                    
                    // Cache now available
                    this.hasCache = true;
                }
            }
        }

        // Return the cache
        return this.cache.stream();
    }

    /**
     * Returns the retriever.
     * 
     * @return the retriever
     */

    private Supplier<Stream<T>> getRetriever()
    {
        return this.retriever;
    }

    /**
     * Hidden constructor.
     * 
     * @param retriever the retriever
     */

    private CachingRetriever( Supplier<Stream<T>> retriever )
    {
        this.hasCache = false;
        this.lock = new Object();
        this.retriever = retriever;
    }

}

package wres.io.retrieval.datashop;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Adds a wrapper to a {@link Retriever} to allow the retrieved data to be cached locally for re-use. On the second and
 * further calls, the cached data is returned. Can optionally build to retrieve on each call, allowing this facade to 
 * be used in all contexts, caching or otherwise. 
 * 
 * @author james.brown@hydrosolved.com
 * @param <T> the type of data to retrieve
 */

public class SupplyOrRetrieve<T> implements Supplier<List<T>>
{

    /**
     * Status of the cache. If <code>true</code>, the cache is available, <code>false</code> otherwise.
     */

    private final AtomicBoolean hasCache;

    /**
     * If <code>true</code>, cache.
     */

    private final AtomicBoolean shouldCache;

    /**
     * Lock object.
     */

    private final Object lock;

    /**
     * Retriever.
     */

    private final Retriever<T> retriever;

    /**
     * Cache of data.
     */

    private List<T> cache;

    /**
     * Hidden constructor.
     * 
     * @param <T> the type of data to retrieve
     * @param retriever the retriever
     * @param cache is true to cache the retrieved data for re-use
     * @return the supplier
     */

    public static <T> SupplyOrRetrieve<T> of( Retriever<T> retriever, boolean cache )
    {
        return new SupplyOrRetrieve<>( retriever, cache );
    }

    @Override
    public List<T> get()
    {
        // Should cache?
        if ( this.shouldCache.get() )
        {
            // Has cache?
            if ( !this.hasCache.get() )
            {
                // Lock, in order to prepare cache
                synchronized ( this.lock )
                {
                    this.cache = this.getRetriever()
                                     .getAll()
                                     .collect( Collectors.toUnmodifiableList() );
                    
                    // Cache available
                    this.hasCache.set( true );
                }
            }

            // Return the cache
            return this.cache;
        }
        // Retrieve afresh
        else
        {
            return this.getRetriever()
                       .getAll()
                       .collect( Collectors.toUnmodifiableList() );
        }
    }

    /**
     * Returns the retriever.
     * 
     * @return the retriever
     */

    private Retriever<T> getRetriever()
    {
        return this.retriever;
    }

    /**
     * Hidden constructor.
     * 
     * @param retriever the retriever
     * @param cache is true to cache the retrieved data for re-use
     */

    private SupplyOrRetrieve( Retriever<T> retriever, boolean cache )
    {
        this.hasCache = new AtomicBoolean();
        this.shouldCache = new AtomicBoolean( cache );
        this.lock = new Object();
        this.retriever = retriever;
    }

}

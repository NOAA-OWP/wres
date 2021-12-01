package wres.io.retrieval;

import java.util.Objects;
import java.util.function.Supplier;

import net.jcip.annotations.Immutable;

/**
 * <p>Adds a wrapper to a {@link Supplier} to cache the result so that the second and subsequent calls return the cached
 * result.
 * 
 * @author James Brown
 * @param <T> the type of data to retrieve
 */
@Immutable
public class CachingSupplier<T> implements Supplier<T>
{

    /** Cache of data.*/
    private volatile T cache;

    /** The underlying retriever. */
    private final Supplier<T> retriever;

    /**
     * Provides an instance.
     * 
     * @param <T> the type of data to retrieve
     * @param retriever the retriever
     * @return a caching retriever
     * @throws NullPointerException if the retriever is null
     */

    public static <T> CachingSupplier<T> of( Supplier<T> retriever )
    {
        return new CachingSupplier<>( retriever );
    }

    @Override
    public T get()
    {
        // Double-checked locking idiom with optimization to check the volatile cache only once
        T localCache = this.cache;
        if ( Objects.isNull( localCache ) )
        {
            synchronized ( this )
            {
                localCache = this.cache;
                if ( Objects.isNull( localCache ) )
                {
                    this.cache = localCache = this.retriever.get();
                }
            }
        }

        return this.cache;
    }

    /**
     * Hidden constructor.
     * 
     * @param retriever the retriever
     * @throws NullPointerException if the retriever is null
     */

    private CachingSupplier( Supplier<T> retriever )
    {
        Objects.requireNonNull( retriever );

        this.retriever = retriever;
    }

}

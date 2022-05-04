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

    /** The underlying supplier. */
    private final Supplier<T> supplier;

    /**
     * Provides an instance.
     * 
     * @param <T> the type of data to supply
     * @param supplier the supplier
     * @return a caching supplier
     * @throws NullPointerException if the supplier is null
     */

    public static <T> CachingSupplier<T> of( Supplier<T> supplier )
    {
        return new CachingSupplier<>( supplier );
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
                    this.cache = localCache = this.supplier.get();
                }
            }
        }

        return localCache;
    }

    /**
     * Hidden constructor.
     * 
     * @param supplier the supplier
     * @throws NullPointerException if the retriever is null
     */

    private CachingSupplier( Supplier<T> supplier )
    {
        Objects.requireNonNull( supplier );

        this.supplier = supplier;
    }

}

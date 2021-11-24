package wres.io.retrieval;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.jcip.annotations.Immutable;

/**
 * <p>Adds a wrapper to a {@link Supplier} that supplies a {@link Stream} of retrieved data. This allows the retrieved 
 * data to be cached locally for re-use. The data is acquired eagerly, on construction.
 * 
 * @author James Brown
 * @param <T> the type of data to retrieve
 */
@Immutable
public class CachingRetriever<T> implements Supplier<Stream<T>>
{

    /** Cache of data.*/
    private volatile List<T> cache;

    /** The underlying retriever. */
    private final Supplier<Stream<T>> retriever;

    /**
     * Provides an instance.
     * 
     * @param <T> the type of data to retrieve
     * @param retriever the retriever
     * @return a caching retriever
     * @throws NullPointerException if the retriever is null
     */

    public static <T> CachingRetriever<T> of( Supplier<Stream<T>> retriever )
    {
        return new CachingRetriever<>( retriever );
    }

    @Override
    public Stream<T> get()
    {
        // Double-checked locking idiom with optimization to check the volatile cache only once
        List<T> localCache = this.cache;
        if ( Objects.isNull( localCache ) )
        {
            synchronized ( this )
            {
                localCache = this.cache;
                if ( Objects.isNull( localCache ) )
                {
                    this.cache = localCache = this.retriever.get()
                                                            .collect( Collectors.toUnmodifiableList() );
                }
            }
        }

        return this.cache.stream();
    }

    /**
     * Hidden constructor.
     * 
     * @param retriever the retriever
     * @throws NullPointerException if the retriever is null
     */

    private CachingRetriever( Supplier<Stream<T>> retriever )
    {
        Objects.requireNonNull( retriever );

        this.retriever = retriever;
    }

}

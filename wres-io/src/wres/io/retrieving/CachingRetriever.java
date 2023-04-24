package wres.io.retrieving;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.jcip.annotations.Immutable;

/**
 * <p>Adds a wrapper to a {@link Supplier} that supplies a {@link Stream} of retrieved data. Similar to the
 * {@link CachingSupplier} and uses this implementation to cache the result. However, unlike the
 * {@link CachingSupplier}, this class creates a new stream on each call, allowing for re-use of the retriever.
 * 
 * @author James Brown
 * @param <T> the type of data to retrieve
 */
@Immutable
public class CachingRetriever<T> implements Supplier<Stream<T>>
{
    /** The underlying cached supply. */
    private final Supplier<List<T>> cachingSupplier;

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
        return this.cachingSupplier.get()
                                   .stream();
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

        this.cachingSupplier = CachingSupplier.of( () -> retriever.get()
                                                                  .toList() );
    }

}

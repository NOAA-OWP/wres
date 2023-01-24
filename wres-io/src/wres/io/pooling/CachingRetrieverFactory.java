package wres.io.pooling;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.retrieval.CachingRetriever;
import wres.io.retrieval.RetrieverFactory;

/**
 * Implementation of a {@link RetrieverFactory} that delegates all calls to a factory supplied on construction, but 
 * wraps calls to any data sources that should be cached with a {@link CachingRetriever} and caches them locally. In 
 * other words, retrieval should be cached for those instances, regardless of whether the cached instance is further 
 * cached locally and re-used across pools or there are repeated calls to the factory methods. However, in the current 
 * pattern, there is one such factory instance for each feature group, so it should not be necessary to cache more than 
 * one retriever (i.e., multiple requests will always consider the same collection of features). Thus, the size of the 
 * cache is currently one for each type of retrieval. Uses a coarse-grained write lock on creating cached values that 
 * reflects the current usage pattern.
 *  
 * @param <L> the left data type
 * @param <R> the right data type
 */

class CachingRetrieverFactory<L, R> implements RetrieverFactory<L, R>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( CachingRetrieverFactory.class );

    /** The factory to delegate to. */
    private final RetrieverFactory<L, R> delegate;

    /** Whether the baseline is a generated baseline and should, therefore, be cached across pools. */
    private final boolean hasGeneratedBaseline;

    /** Whether the baseline data source is the same as the climatological data source. */
    private final boolean hasEqualBaselineAndClimatology;

    /** Function to map between climatology and baseline when they are equal. */
    private final Function<TimeSeries<L>, TimeSeries<R>> baselineMapper;

    /** Cache of (cached) retrievers for climatology. */
    private final Cache<Key, Supplier<Stream<TimeSeries<L>>>> climatologyCache =
            Caffeine.newBuilder()
                    .maximumSize( 1 )
                    .build();

    /** Cache of (cached) retrievers for generated baselines. */
    private final Cache<Key, Supplier<Stream<TimeSeries<R>>>> generatedBaselineCache =
            Caffeine.newBuilder()
                    .maximumSize( 1 )
                    .build();

    /** Lock for creating a cached retriever of generated baseline data. TODO: consider a finer grained lock per 
     * cached key (feature collection and/or time window) if the usage pattern changes from the pattern described 
     * in the class header.*/
    private final ReentrantLock generatedBaselineWriteLock = new ReentrantLock();

    /** Lock for creating a cached retriever of climatological data. TODO: consider a finer grained lock per cached 
     * key (feature collection and/or time window) if the usage pattern changes from the pattern described in the 
     * class header.*/
    private final ReentrantLock climatologyWriteLock = new ReentrantLock();

    @Override
    public Supplier<Stream<TimeSeries<L>>> getClimatologyRetriever( Set<Feature> features )
    {
        Objects.requireNonNull( features );

        Key key = new Key( features );
        Supplier<Stream<TimeSeries<L>>> cached = this.climatologyCache.getIfPresent( key );

        if ( Objects.isNull( cached ) )
        {
            try
            {
                this.climatologyWriteLock.lock();

                // Check again for any thread waiting between the first null check and the lock
                cached = this.climatologyCache.getIfPresent( key );
                if ( Objects.isNull( cached ) )
                {
                    LOGGER.debug( "Retrieving climatological data for features: {}.", features );

                    Supplier<Stream<TimeSeries<L>>> delegated = this.delegate.getClimatologyRetriever( features );
                    cached = CachingRetriever.of( delegated );
                    this.climatologyCache.put( key, cached );
                }
            }
            finally
            {
                this.climatologyWriteLock.unlock();
            }
        }

        return cached;
    }

    @Override
    public Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<Feature> features )
    {
        return this.delegate.getLeftRetriever( features );
    }

    @Override
    public Supplier<Stream<TimeSeries<R>>> getBaselineRetriever( Set<Feature> features )
    {
        Objects.requireNonNull( features );

        // Same data as climatological source?
        if ( this.hasEqualBaselineAndClimatology )
        {
            return () -> {
                Supplier<Stream<TimeSeries<L>>> climatology = this.getClimatologyRetriever( features );
                return climatology.get().map( this.baselineMapper );
            };
        }

        // Generated baseline? If so, cache and return.
        if ( this.hasGeneratedBaseline )
        {
            Key key = new Key( features );
            Supplier<Stream<TimeSeries<R>>> cached = this.generatedBaselineCache.getIfPresent( key );

            if ( Objects.isNull( cached ) )
            {
                try
                {
                    this.generatedBaselineWriteLock.lock();

                    // Check again for any thread waiting between the first null check and the lock
                    cached = this.generatedBaselineCache.getIfPresent( key );
                    if ( Objects.isNull( cached ) )
                    {
                        LOGGER.debug( "Retrieving baseline data for features: {}.", features );

                        Supplier<Stream<TimeSeries<R>>> delegated = this.delegate.getBaselineRetriever( features );
                        cached = CachingRetriever.of( delegated );
                        this.generatedBaselineCache.put( key, cached );
                    }
                }
                finally
                {
                    this.generatedBaselineWriteLock.unlock();
                }
            }

            return cached;
        }

        return this.delegate.getBaselineRetriever( features );
    }

    @Override
    public Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<Feature> features, TimeWindowOuter timeWindow )
    {
        return this.delegate.getLeftRetriever( features, timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<R>>> getRightRetriever( Set<Feature> features, TimeWindowOuter timeWindow )
    {
        return this.delegate.getRightRetriever( features, timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<R>>> getBaselineRetriever( Set<Feature> features,
                                                                 TimeWindowOuter timeWindow )
    {
        return this.delegate.getBaselineRetriever( features, timeWindow );
    }

    /**
     * @param delegate the factory to delegate to
     * @param hasGeneratedBaseline whether the baseline is part of a generated baseline
     * @param hasEqualBaselineAndClimatology whether the baseline and climatological data are the same
     * @throws NullPointerException if any required input is null
     */
    CachingRetrieverFactory( RetrieverFactory<L, R> delegate,
                             boolean hasGeneratedBaseline,
                             boolean hasEqualBaselineAndClimatology,
                             Function<TimeSeries<L>, TimeSeries<R>> baselineMapper )
    {
        Objects.requireNonNull( delegate );

        if ( hasEqualBaselineAndClimatology )
        {
            Objects.requireNonNull( baselineMapper );
        }

        this.delegate = delegate;
        this.hasGeneratedBaseline = hasGeneratedBaseline;
        this.hasEqualBaselineAndClimatology = hasEqualBaselineAndClimatology;
        this.baselineMapper = baselineMapper;
    }

    /**
     * A cache key. TODO: add a time window if/when required.
     */
    private record Key( Set<Feature> features )
    {
    }
}

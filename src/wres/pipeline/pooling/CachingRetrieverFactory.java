package wres.pipeline.pooling;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.protobuf.Duration;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.config.DeclarationUtilities;
import wres.config.components.Dataset;
import wres.config.components.DatasetOrientation;
import wres.config.components.Source;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.project.Project;
import wres.io.retrieving.CachingRetriever;
import wres.io.retrieving.RetrieverFactory;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeWindow;

/**
 * Implementation of a {@link RetrieverFactory} that delegates all calls to a factory supplied on construction, but 
 * wraps calls to any data sources that should be cached with a {@link CachingRetriever} and caches them locally. In 
 * other words, retrieval should be cached for those instances, regardless of whether the cached instance is further 
 * cached locally and re-used across pools or there are repeated calls to the factory methods. However, in the current 
 * pattern, there is one such factory instance for each feature group, so it should not be necessary to cache more than 
 * one retriever (i.e., multiple requests will always consider the same collection of features). Thus, the size of the 
 * cache is currently one for each type of retrieval, except covariates which can have multiple variables. Uses a
 * coarse-grained write lock on creating cached values that reflects the current usage pattern.
 *
 * @param <L> the left data type
 * @param <R> the right data type
 * @param <B> the baseline data type
 */

class CachingRetrieverFactory<L, R, B> implements RetrieverFactory<L, R, B>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( CachingRetrieverFactory.class );

    /** The factory to delegate to. */
    private final RetrieverFactory<L, R, B> delegate;

    /** The project. */
    private final Project project;

    /** Whether the baseline data source is the same as the left-ish data source. */
    private final boolean hasEqualBaselineAndLeft;

    /** Function to map between left and baseline when they are equal. */
    private final Function<TimeSeries<L>, TimeSeries<B>> baselineMapper;

    /** Cache of retrievers for left-ish data. */
    private final Cache<@NonNull Key, Supplier<Stream<TimeSeries<L>>>> leftCache =
            Caffeine.newBuilder()
                    .maximumSize( 1 )
                    .build();

    /** Cache of retrievers for covariate data. */
    private final Cache<@NonNull Key, Supplier<Stream<TimeSeries<L>>>> covariateCache =
            Caffeine.newBuilder()
                    .maximumSize( 100 )  // Arbitrarily
                    .build();

    /** Cache of retrievers for generated baselines. */
    private final Cache<@NonNull Key, Supplier<Stream<TimeSeries<B>>>> generatedBaselineCache =
            Caffeine.newBuilder()
                    .maximumSize( 1 )
                    .build();

    /** Lock for creating a cached retriever of generated baseline data.*/
    private final ReentrantLock generatedBaselineWriteLock = new ReentrantLock();

    /** Lock for creating a cached retriever of left-ish data.*/
    private final ReentrantLock leftWriteLock = new ReentrantLock();

    /** Lock for creating a cached retriever of covariate data. */
    private final ReentrantLock covariateWriteLock = new ReentrantLock();

    @Override
    public Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<Feature> features )
    {
        return this.getCachedSupplier( features,
                                       null,
                                       null,
                                       this.leftCache,
                                       this.leftWriteLock,
                                       () -> this.delegate.getLeftRetriever( features ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<B>>> getBaselineRetriever( Set<Feature> features )
    {
        Objects.requireNonNull( features );

        // Same data as left-ish data?
        if ( this.hasEqualBaselineAndLeft )
        {
            return () -> {
                Supplier<Stream<TimeSeries<L>>> climatology = this.getLeftRetriever( features );
                return climatology.get()
                                  .map( this.baselineMapper );
            };
        }

        // Generated baseline? If so, cache and return.
        if ( this.project.hasGeneratedBaseline() )
        {
            return this.getCachedSupplier( features,
                                           null,
                                           null,
                                           this.generatedBaselineCache,
                                           this.generatedBaselineWriteLock,
                                           () -> this.delegate.getBaselineRetriever( features ) );
        }

        return this.delegate.getBaselineRetriever( features );
    }

    @Override
    public Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<Feature> features, TimeWindowOuter timeWindow )
    {
        TimeWindowOuter timeWindowForCache = this.getTimeWindowForCache( timeWindow,
                                                                         DatasetOrientation.LEFT,
                                                                         null );

        return this.getCachedSupplier( features,
                                       null,
                                       timeWindowForCache,
                                       this.leftCache,
                                       this.leftWriteLock,
                                       () -> this.delegate.getLeftRetriever( features, timeWindow ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<R>>> getRightRetriever( Set<Feature> features, TimeWindowOuter timeWindow )
    {
        return this.delegate.getRightRetriever( features, timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<B>>> getBaselineRetriever( Set<Feature> features, TimeWindowOuter timeWindow )
    {
        return this.delegate.getBaselineRetriever( features, timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<L>>> getCovariateRetriever( Set<Feature> features, String variableName )
    {
        return this.getCachedSupplier( features,
                                       variableName,
                                       null,
                                       this.covariateCache,
                                       this.covariateWriteLock,
                                       () -> this.delegate.getCovariateRetriever( features,
                                                                                  variableName ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<L>>> getCovariateRetriever( Set<Feature> features,
                                                                  String variableName,
                                                                  TimeWindowOuter timeWindow )
    {
        TimeWindowOuter timeWindowForCache = this.getTimeWindowForCache( timeWindow,
                                                                         DatasetOrientation.COVARIATE,
                                                                         variableName );

        return this.getCachedSupplier( features,
                                       variableName,
                                       timeWindowForCache,
                                       this.covariateCache,
                                       this.covariateWriteLock,
                                       () -> this.delegate.getCovariateRetriever( features,
                                                                                  variableName,
                                                                                  timeWindow ) );
    }

    /**
     * Generates a supplier, looking in the cache first and adding to the cache when absent.
     *
     * @param features the features for which data are required
     * @param variableName the name of the variable for which data is required
     * @param timeWindow the time window
     * @param cache the cache
     * @param lock the lock
     * @param uncachedSupplier the raw/uncached data supply
     * @param <T> the data type
     * @return the supplier
     */

    private <T> Supplier<Stream<TimeSeries<T>>> getCachedSupplier( Set<Feature> features,
                                                                   String variableName,
                                                                   TimeWindowOuter timeWindow,
                                                                   Cache<@NonNull Key, Supplier<Stream<TimeSeries<T>>>> cache,
                                                                   ReentrantLock lock,
                                                                   Supplier<Supplier<Stream<TimeSeries<T>>>> uncachedSupplier )
    {
        Key key = new Key( features, variableName, timeWindow );
        Supplier<Stream<TimeSeries<T>>> cached = cache.getIfPresent( key );

        if ( Objects.isNull( cached ) )
        {
            try
            {
                lock.lock();

                // Check again for any thread waiting between the first null check and the lock
                cached = cache.getIfPresent( key );
                if ( Objects.isNull( cached ) )
                {
                    LOGGER.debug( "Adding a cached time-series dataset for features: {} and variable: {}.",
                                  features,
                                  variableName );

                    Supplier<Stream<TimeSeries<T>>> delegated = uncachedSupplier.get();
                    cached = CachingRetriever.of( delegated );
                    cache.put( key, cached );
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        else if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Retrieving a cached time-series dataset for features: {} and variable: {}.",
                          features,
                          variableName );
        }

        return cached;
    }

    /**
     * @return whether the baseline and left-ish data sources are equal and can be de-duplicated on retrieval
     */

    private boolean hasEqualBaselineAndLeft( Project project )
    {
        if ( !project.hasGeneratedBaseline() )
        {
            return false;
        }

        if ( !Objects.equals( project.getLeftVariable(),
                              project.getBaselineVariable() ) )
        {
            return false;
        }

        // Are the declared data sources equal?
        // A common assumption throughout WRES is that sources can be de-duplicated on the basis of declaration and that
        // any runtime differences, based on when reading/ingest calls an external source for a snapshot, should be
        // ignored - in other words, the first snapshot wins, because this allows for de-duplication
        Dataset left = DeclarationUtilities.getDeclaredDataset( project.getDeclaration(),
                                                                DatasetOrientation.LEFT );
        List<Source> baselineSources = null;
        if ( project.hasBaseline() )
        {
            baselineSources = DeclarationUtilities.getDeclaredDataset( project.getDeclaration(),
                                                                       DatasetOrientation.BASELINE )
                                                  .sources();
        }
        return Objects.equals( left.sources(), baselineSources );
    }

    /**
     * Generates a time window for caching purposes, which retains only the relevant parts of the input window that can
     * be deduced from the project supplied on construction. By removing irrelevant parts, such as lead durations for
     * non-forecasts, the opportunities for caching are maximized when different detailed requests for data (e.g.,
     * different lead durations) inevitably deliver the same underlying dataset. Such requests occur when they are part
     * of an automated pipeline in which the lead durations are relevant for some other aspects.
     *
     * @param timeWindow the time window
     * @param orientation the dataset orientation
     * @param variableName the variable name
     * @return the rationalized time window for caching purposes
     */

    private TimeWindowOuter getTimeWindowForCache( TimeWindowOuter timeWindow,
                                                   DatasetOrientation orientation,
                                                   String variableName )
    {
        switch ( orientation )
        {
            case LEFT ->
            {
                if ( this.project.getDeclaration()
                                 .left()
                                 .type()
                                 .isForecastType() )
                {
                    return timeWindow;
                }

                // Remove the lead durations
                return this.clearLeadDurations( timeWindow );
            }
            case COVARIATE ->
            {
                if ( this.project.getCovariateDataset( variableName )
                                 .dataset()
                                 .type()
                                 .isForecastType() )
                {
                    return timeWindow;
                }

                // Remove the lead durations
                return this.clearLeadDurations( timeWindow );
            }
            default -> throw new UnsupportedOperationException( "Only 'observed' and 'covariate' datasets are "
                                                                + "currently supported for caching by time window." );
        }
    }

    /**
     * Clears the lead durations from the input time window.
     * @param timeWindow the time window
     * @return the time window with lead durations cleared
     */

    private TimeWindowOuter clearLeadDurations( TimeWindowOuter timeWindow )
    {
        Duration lowerBound = MessageUtilities.getDuration( MessageUtilities.DURATION_MIN );
        Duration upperBound = MessageUtilities.getDuration( MessageUtilities.DURATION_MAX );

        return TimeWindowOuter.of( TimeWindow.newBuilder( timeWindow.getTimeWindow() )
                                             .setEarliestLeadDuration( lowerBound )
                                             .setLatestLeadDuration( upperBound )
                                             .build() );
    }

    /**
     * A cache key.
     * @param features the features
     * @param variableName the variable name
     * @param timeWindow the time window
     */
    private record Key( Set<Feature> features, String variableName, TimeWindowOuter timeWindow )
    {
    }

    /**
     * @param delegate the factory to delegate to, required
     * @param project a project to help with determining what caching can be performed, required
     * @param baselineMapper a mapper between potentially exchangeable datasets, specifically left and baseline
     * @throws NullPointerException if any required input is null
     */
    CachingRetrieverFactory( RetrieverFactory<L, R, B> delegate,
                             Project project,
                             Function<TimeSeries<L>, TimeSeries<B>> baselineMapper )
    {
        Objects.requireNonNull( delegate );
        Objects.requireNonNull( project );

        this.hasEqualBaselineAndLeft = this.hasEqualBaselineAndLeft( project );

        if ( this.hasEqualBaselineAndLeft )
        {
            Objects.requireNonNull( baselineMapper, "A baseline mapper is required to allow for de-"
                                                    + "duplication of left and baseline datasets that are the same." );
        }

        this.delegate = delegate;
        this.baselineMapper = baselineMapper;
        this.project = project;
    }
}

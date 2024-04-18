package wres.pipeline.pooling;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.CovariateDataset;
import wres.datamodel.pools.Pool;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.RescaledTimeSeriesPlusValidation;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;

/**
 * Filters a pool against a covariate dataset.
 *
 * @author James Brown
 */
class CovariateFilter<L, R> implements Supplier<Pool<TimeSeries<Pair<L, R>>>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( CovariateFilter.class );

    /** The pool to filter against a covariate. */
    private final Pool<TimeSeries<Pair<L, R>>> pool;

    /** The description of the covariate dataset. */
    private final Covariate<L> covariate;

    private final Supplier<Stream<TimeSeries<L>>> covariateData;

    @Override
    public Pool<TimeSeries<Pair<L, R>>> get()
    {
        // Read the covariate data
        try ( Stream<TimeSeries<L>> covariateStream = this.covariateData.get() )
        {
            List<TimeSeries<L>> covariateSeries = covariateStream.toList();

            Pool.Builder<TimeSeries<Pair<L, R>>> filteredPool =
                    new Pool.Builder<TimeSeries<Pair<L, R>>>().setMetadata( this.pool.getMetadata() )
                                                              .setClimatology( this.pool.getClimatology() );

            if ( this.pool.hasBaseline() )
            {
                filteredPool.setMetadataForBaseline( this.pool.getBaselineData()
                                                              .getMetadata() );
            }

            // Iterate through each mini-pool by geographic feature
            List<Pool<TimeSeries<Pair<L, R>>>> miniPools = this.pool.getMiniPools();
            for ( Pool<TimeSeries<Pair<L, R>>> nextPool : miniPools )
            {
                Pool<TimeSeries<Pair<L, R>>> filteredMiniPool = this.applyCovariate( nextPool,
                                                                                     this.covariate,
                                                                                     covariateSeries );
                filteredPool.addPool( filteredMiniPool );
            }

            return filteredPool.build();
        }
    }

    /**
     * Filters are cross-pairs the supplied pool against the covariate dataset.
     *
     * @param pool the pool
     * @param covariate the covariate dataset
     * @param covariateSeries the covariate time-series
     * @return the filtered pool
     */
    private Pool<TimeSeries<Pair<L, R>>> applyCovariate( Pool<TimeSeries<Pair<L, R>>> pool,
                                                         Covariate<L> covariate,
                                                         List<TimeSeries<L>> covariateSeries )
    {
        // Find the valid times at which upscaled values must end when upscaling is required
        SortedSet<Instant> upscaledEndsAt = pool.get()
                                                .stream()
                                                .flatMap( t -> t.getEvents()
                                                                .stream()
                                                                .map( Event::getTime ) )
                                                .collect( Collectors.toCollection( TreeSet::new ) );

        // Get the feature with the same name and feature authority as the covariate dataset. Note that other
        // attributes of the feature, such as coordinates, may differ
        Feature feature = this.getFeatureName( pool, covariate.datasetDescription() );
        List<TimeSeries<L>> featuredCovariate = covariateSeries.stream()
                                                               .filter( t -> Objects.equals( feature.getName(),
                                                                                             t.getMetadata()
                                                                                              .getFeature()
                                                                                              .getName() ) )
                                                               .toList();

        // Upscale the covariate time-series as needed
        List<TimeSeries<L>> filteredAndUpscaledSeries = new ArrayList<>();
        for ( TimeSeries<L> series : featuredCovariate )
        {
            boolean upscale = Objects.nonNull( covariate.desiredTimeScale() )
                              && Objects.nonNull( series.getTimeScale() )
                              && TimeScaleOuter.isRescalingRequired( series.getTimeScale(),
                                                                     covariate.desiredTimeScale() )
                              && !covariate.desiredTimeScale()
                                           .equals( series.getTimeScale() );

            if ( upscale )
            {
                RescaledTimeSeriesPlusValidation<L> rescaled = covariate.upscaler()
                                                                        .upscale( series,
                                                                                  covariate.desiredTimeScale(),
                                                                                  upscaledEndsAt,
                                                                                  series.getMetadata()
                                                                                        .getUnit() );

                LOGGER.debug( "Encountered the following status events when upscaling time-series for covariate {}: "
                              + "{}.",
                              covariate.datasetDescription()
                                       .dataset()
                                       .variable()
                                       .name(),
                              rescaled.getValidationEvents() );

                // Re-assign the series
                series = rescaled.getTimeSeries();
            }

            // Filter the series using the covariate filter
            series = TimeSeriesSlicer.filter( series, covariate.filter() );
            filteredAndUpscaledSeries.add( series );
        }

        // Cross-pair
        Pool.Builder<TimeSeries<Pair<L, R>>> poolBuilder =
                new Pool.Builder<TimeSeries<Pair<L, R>>>().setMetadata( pool.getMetadata() )
                                                          .setClimatology( pool.getClimatology() );
        Set<Instant> validTimes = filteredAndUpscaledSeries.stream()
                                                           .flatMap( t -> t.getEvents()
                                                                           .stream() )
                                                           .map( Event::getTime )
                                                           .collect( Collectors.toSet() );
        List<TimeSeries<Pair<L, R>>> poolData = pool.get();
        List<TimeSeries<Pair<L, R>>> crossPaired = poolData.stream()
                                                           .map( n -> this.filterByValidTime( n, validTimes ) )
                                                           .toList();
        poolBuilder.addData( crossPaired );

        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
            List<TimeSeries<Pair<L, R>>> poolDataBaseline = pool.getBaselineData()
                                                                .get();
            List<TimeSeries<Pair<L, R>>> crossPairedBaseline =
                    poolDataBaseline.stream()
                                    .map( n -> this.filterByValidTime( n, validTimes ) )
                                    .toList();
            poolBuilder.addDataForBaseline( crossPairedBaseline );
        }

        return poolBuilder.build();
    }

    /**
     * Creates an instance.
     * @param pool the pool
     * @param covariate the covariate description
     * @param covariateData the covariate data
     * @throws NullPointerException if any input is null
     */
    static <L, R> CovariateFilter<L, R> of( Pool<TimeSeries<Pair<L, R>>> pool,
                                            Covariate<L> covariate,
                                            Supplier<Stream<TimeSeries<L>>> covariateData )
    {
        return new CovariateFilter<>( pool, covariate, covariateData );
    }

    /**
     * Returns the name of the first feature in the pool that has the same feature authority as the covariate dataset,
     * defaulting to the feature authority associated with the
     * {@link wres.config.yaml.components.DatasetOrientation#LEFT} data.
     *
     * @param pool the pool
     * @param covariateDescription the covariate dataset description
     * @return the feature
     */

    private Feature getFeatureName( Pool<TimeSeries<Pair<L, R>>> pool,
                                    CovariateDataset covariateDescription )
    {
        FeatureTuple featureTuple = pool.getMetadata()
                                        .getFeatureTuples()
                                        .iterator()
                                        .next();

        switch ( covariateDescription.featureNameOrientation() )
        {
            case LEFT ->
            {
                return featureTuple.getLeft();
            }
            case RIGHT ->
            {
                return featureTuple.getRight();
            }
            case BASELINE ->
            {
                return featureTuple.getBaseline();
            }
            default -> throw new IllegalStateException( "Unrecognized dataset orientation, '"
                                                        + covariateDescription.featureNameOrientation()
                                                        + "'." );
        }
    }

    /**
     * Retains only those events in the input time-series that have corresponding valid times in the supplied set.
     * @param timeSeries the time-series to filter
     * @param validTimes the valid times to retain
     * @return the filtered time-series, possibly empty
     */

    private TimeSeries<Pair<L, R>> filterByValidTime( TimeSeries<Pair<L, R>> timeSeries, Set<Instant> validTimes )
    {

        SortedSet<Event<Pair<L, R>>> events = timeSeries.getEvents()
                                                        .stream()
                                                        .filter( nextEvent -> validTimes.contains( nextEvent.getTime() ) )
                                                        .collect( Collectors.toCollection( TreeSet::new ) );

        return new TimeSeries.Builder<Pair<L, R>>().setMetadata( timeSeries.getMetadata() )
                                                   .setEvents( events )
                                                   .build();
    }

    /**
     * Creates an instance.
     * @param pool the pool
     * @param covariate the covariate description
     * @param covariateData the covariate data
     * @throws NullPointerException if any input is null
     */
    private CovariateFilter( Pool<TimeSeries<Pair<L, R>>> pool,
                             Covariate<L> covariate,
                             Supplier<Stream<TimeSeries<L>>> covariateData )
    {
        Objects.requireNonNull( pool );
        Objects.requireNonNull( covariate );
        Objects.requireNonNull( covariateData );

        this.pool = pool;
        this.covariate = covariate;
        this.covariateData = covariateData;
    }
}
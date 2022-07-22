package wres.datamodel.time;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.Ensemble;
import wres.datamodel.space.FeatureKey;

/**
 * Store of {@link TimeSeries} that is built incrementally with a {@link Builder}.
 * @author James Brown
 */

@Immutable
public class TimeSeriesStore
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesStore.class );

    /** Left-ish time-series of {@link Double}. **/
    private final List<TimeSeries<Double>> leftSingleValuedSeries;

    /** Right-ish time-series of {@link Double}. **/
    private final List<TimeSeries<Double>> rightSingleValuedSeries;

    /** Baseline-ish time-series of {@link Double}. **/
    private final List<TimeSeries<Double>> baselineSingleValuedSeries;

    /** Left-ish time-series of {@link Ensemble}. **/
    private final List<TimeSeries<Ensemble>> leftEnsembleSeries;

    /** Right-ish time-series of {@link Ensemble}. **/
    private final List<TimeSeries<Ensemble>> rightEnsembleSeries;

    /** Baseline-ish time-series of {@link Ensemble}. **/
    private final List<TimeSeries<Ensemble>> baselineEnsembleSeries;

    /**
     * Returns all single-valued series by feature.
     * @param orientation the orientation
     * @param features the features
     * @return the filtered series
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    public Stream<TimeSeries<Double>> getSingleValuedSeries( LeftOrRightOrBaseline orientation,
                                                             Set<FeatureKey> features )
    {
        Objects.requireNonNull( orientation );
        Objects.requireNonNull( features );

        return TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                     this.rightSingleValuedSeries,
                                                     this.baselineSingleValuedSeries,
                                                     orientation )
                              .stream()
                              .filter( next -> features.contains( next.getMetadata().getFeature() ) );
    }

    /**
     * Returns all single-valued series for a given orientation.
     * @param orientation the orientation
     * @return the oriented time series
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    public Stream<TimeSeries<Double>> getSingleValuedSeries( LeftOrRightOrBaseline orientation )
    {
        Objects.requireNonNull( orientation );

        return TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                     this.rightSingleValuedSeries,
                                                     this.baselineSingleValuedSeries,
                                                     orientation )
                              .stream();
    }

    /**
     * Returns all single-valued series.
     * @return the time series
     */

    public Stream<TimeSeries<Double>> getSingleValuedSeries()
    {
        return Stream.concat( Stream.concat( this.leftSingleValuedSeries.stream(),
                                             this.rightSingleValuedSeries.stream() ),
                              this.baselineSingleValuedSeries.stream() );
    }

    /**
     * Filters the single-valued series by time window and feature.
     * @param timeWindow the time window
     * @param orientation the orientation
     * @param features the features
     * @return the filtered series
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    public Stream<TimeSeries<Double>> getSingleValuedSeries( TimeWindowOuter timeWindow,
                                                             LeftOrRightOrBaseline orientation,
                                                             Set<FeatureKey> features )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( orientation );
        Objects.requireNonNull( features );
        
        return TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                     this.rightSingleValuedSeries,
                                                     this.baselineSingleValuedSeries,
                                                     orientation )
                              .stream()
                              .filter( next -> features.contains( next.getMetadata().getFeature() ) )
                              .map( next -> TimeSeriesSlicer.filter( next, timeWindow ) );
    }

    /**
     * Filters the ensemble series by time window and feature.
     * @param timeWindow the time window
     * @param orientation the context
     * @param features the features
     * @return the filtered series
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    public Stream<TimeSeries<Ensemble>> getEnsembleSeries( TimeWindowOuter timeWindow,
                                                           LeftOrRightOrBaseline orientation,
                                                           Set<FeatureKey> features )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( orientation );
        Objects.requireNonNull( features );

        return TimeSeriesStore.getEnsembleStore( this.leftEnsembleSeries,
                                                 this.rightEnsembleSeries,
                                                 this.baselineEnsembleSeries,
                                                 orientation )
                              .stream()
                              .filter( next -> features.contains( next.getMetadata().getFeature() ) )
                              .map( next -> TimeSeriesSlicer.filter( next,
                                                                     timeWindow ) );
    }

    /**
     * Returns all ensemble time-series.
     * @param orientation the orientation
     * @return the time-series
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    public Stream<TimeSeries<Ensemble>> getEnsembleSeries( LeftOrRightOrBaseline orientation )
    {
        Objects.requireNonNull( orientation );

        return TimeSeriesStore.getEnsembleStore( this.leftEnsembleSeries,
                                                 this.rightEnsembleSeries,
                                                 this.baselineEnsembleSeries,
                                                 orientation )
                              .stream();
    }

    /**
     * Returns all ensemble series.
     * @return the time series
     */

    public Stream<TimeSeries<Ensemble>> getEnsembleSeries()
    {
        return Stream.concat( Stream.concat( this.leftEnsembleSeries.stream(),
                                             this.rightEnsembleSeries.stream() ),
                              this.baselineEnsembleSeries.stream() );
    }

    /**
     * Builder to create the store incrementally.
     */

    public static class Builder
    {
        /** Left-ish time-series of {@link Double}. **/
        private final Queue<TimeSeries<Double>> leftSingleValuedSeries = new ConcurrentLinkedQueue<>();

        /** Right-ish time-series of {@link Double}. **/
        private final Queue<TimeSeries<Double>> rightSingleValuedSeries = new ConcurrentLinkedQueue<>();

        /** Baseline-ish time-series of {@link Double}. **/
        private final Queue<TimeSeries<Double>> baselineSingleValuedSeries = new ConcurrentLinkedQueue<>();

        /** Left-ish time-series of {@link Ensemble}. **/
        private final Queue<TimeSeries<Ensemble>> leftEnsembleSeries = new ConcurrentLinkedQueue<>();

        /** Right-ish time-series of {@link Ensemble}. **/
        private final Queue<TimeSeries<Ensemble>> rightEnsembleSeries = new ConcurrentLinkedQueue<>();

        /** Baseline-ish time-series of {@link Ensemble}. **/
        private final Queue<TimeSeries<Ensemble>> baselineEnsembleSeries = new ConcurrentLinkedQueue<>();

        /**
         * Adds a single-valued time-series to the store with given context
         * @param series the time-series
         * @param context the context
         * @return the builder
         * @throws NullPointerException if either input is null
         * @throws IllegalArgumentException if the context is unrecognized
         */

        public Builder addSingleValuedSeries( TimeSeries<Double> series, LeftOrRightOrBaseline context )
        {
            Objects.requireNonNull( series );
            Objects.requireNonNull( context );

            // Strip any reference time type that is ReferenceTimeType.LATEST_OBSERVATION. As of 2022-07-19, this is
            // an artifact of the readers to support ingest into a database schema that does not allow zero reference
            // times. However, reference times of this type are fake (i.e., not part of the original time-series) and 
            // the failure to strip them here will result in these time-series being treated as forecasts
            TimeSeries<Double> adjustedSeries = series;
            if ( series.getReferenceTimes().containsKey( ReferenceTimeType.LATEST_OBSERVATION ) )
            {
                Map<ReferenceTimeType, Instant> referenceTimes = new HashMap<>();
                referenceTimes.putAll( series.getReferenceTimes() );
                referenceTimes.remove( ReferenceTimeType.LATEST_OBSERVATION );

                TimeSeriesMetadata newMetadata = series.getMetadata()
                                                       .toBuilder()
                                                       .setReferenceTimes( referenceTimes )
                                                       .build();
                adjustedSeries = TimeSeries.of( newMetadata, series.getEvents() );
                LOGGER.debug( "Adjusted a left-ish time-series to remove a fake reference time type, {}.",
                              ReferenceTimeType.LATEST_OBSERVATION );
            }

            TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                  this.rightSingleValuedSeries,
                                                  this.baselineSingleValuedSeries,
                                                  context )
                           .add( adjustedSeries );

            return this;
        }

        /**
         * Adds an ensemble time-series to the store with given context
         * @param series the time-series
         * @param context the context
         * @return the builder
         * @throws NullPointerException if either input is null
         * @throws IllegalArgumentException if the context is unrecognized
         */

        public Builder addEnsembleSeries( TimeSeries<Ensemble> series, LeftOrRightOrBaseline context )
        {
            Objects.requireNonNull( series );
            Objects.requireNonNull( context );

            TimeSeriesStore.getEnsembleStore( this.leftEnsembleSeries,
                                              this.rightEnsembleSeries,
                                              this.baselineEnsembleSeries,
                                              context )
                           .add( series );

            return this;
        }

        /**
         * @return a {@link TimeSeriesStore}.
         */

        public TimeSeriesStore build()
        {
            return new TimeSeriesStore( this );
        }
    }

    /**
     * Helper that selects the store based on the context.
     * @param leftSingleValuedSeries the store of left single-valued series
     * @param rightSingleValuedSeries the store of right single-valued series
     * @param baselineSingleValuedSeries the store of baseline single-valued series
     * @param orientation the orientation
     * @return a single-valued store with the prescribed context
     * @throws IllegalArgumentException if the context is unrecognized
     */

    private static Collection<TimeSeries<Double>>
            getSingleValuedStore( Collection<TimeSeries<Double>> leftSingleValuedSeries,
                                  Collection<TimeSeries<Double>> rightSingleValuedSeries,
                                  Collection<TimeSeries<Double>> baselineSingleValuedSeries,
                                  LeftOrRightOrBaseline orientation )
    {
        switch ( orientation )
        {
            case LEFT:
                return leftSingleValuedSeries;
            case RIGHT:
                return rightSingleValuedSeries;
            case BASELINE:
                return baselineSingleValuedSeries;
            default:
                throw new IllegalArgumentException( "Unexpected orientation '" + orientation
                                                    + "'. Expected LEFT or RIGHT or BASELINE." );
        }
    }

    /**
     * Helper that selects the store based on the context.
     * @param leftEnsembleSeries the store of left single-valued series
     * @param rightEnsembleSeries the store of right single-valued series
     * @param baselineEnsembleSeries the store of baseline single-valued series
     * @param context the context
     * @return an ensemble store with the prescribed context
     * @throws IllegalArgumentException if the context is unrecognized
     */

    private static Collection<TimeSeries<Ensemble>>
            getEnsembleStore( Collection<TimeSeries<Ensemble>> leftEnsembleSeries,
                              Collection<TimeSeries<Ensemble>> rightEnsembleSeries,
                              Collection<TimeSeries<Ensemble>> baselineEnsembleSeries,
                              LeftOrRightOrBaseline context )
    {
        switch ( context )
        {
            case LEFT:
                return leftEnsembleSeries;
            case RIGHT:
                return rightEnsembleSeries;
            case BASELINE:
                return baselineEnsembleSeries;
            default:
                throw new IllegalArgumentException( "Unexpected context '" + context
                                                    + "'. Expected LEFT or RIGHT or BASELINE." );
        }
    }

    /**
     * Creates an instance.
     * @param builder the builder
     */

    private TimeSeriesStore( Builder builder )
    {
        // No longer need a concurrent collection type because writing is complete and the collection is now immutable
        this.leftSingleValuedSeries = Collections.unmodifiableList( new ArrayList<>( builder.leftSingleValuedSeries ) );
        this.rightSingleValuedSeries =
                Collections.unmodifiableList( new ArrayList<>( builder.rightSingleValuedSeries ) );
        this.baselineSingleValuedSeries =
                Collections.unmodifiableList( new ArrayList<>( builder.baselineSingleValuedSeries ) );
        this.leftEnsembleSeries = Collections.unmodifiableList( new ArrayList<>( builder.leftEnsembleSeries ) );
        this.rightEnsembleSeries = Collections.unmodifiableList( new ArrayList<>( builder.rightEnsembleSeries ) );
        this.baselineEnsembleSeries = Collections.unmodifiableList( new ArrayList<>( builder.baselineEnsembleSeries ) );

        if ( LOGGER.isInfoEnabled() )
        {
            int size = this.leftSingleValuedSeries.size() + this.rightSingleValuedSeries.size()
                       + this.baselineSingleValuedSeries.size()
                       + this.leftEnsembleSeries.size()
                       + this.rightEnsembleSeries.size()
                       + this.baselineEnsembleSeries.size();

            LOGGER.info( "Created an in-memory time-series store that contains {} time-series. The {} time-series "
                         + "include {} single-valued time-series with a {} orientation, {} single-valued time-series "
                         + "with a {} orientation, {} single-valued time-series with a {} orientation, {} ensemble "
                         + "time-series with a {} orientation, {} ensemble time-series with a {} orientation and {} "
                         + "ensemble time-series with a {} orientation. ",
                         size,
                         size,
                         this.leftSingleValuedSeries.size(),
                         LeftOrRightOrBaseline.LEFT,
                         this.rightSingleValuedSeries.size(),
                         LeftOrRightOrBaseline.RIGHT,
                         this.baselineSingleValuedSeries.size(),
                         LeftOrRightOrBaseline.BASELINE,
                         this.leftEnsembleSeries.size(),
                         LeftOrRightOrBaseline.LEFT,
                         this.rightEnsembleSeries.size(),
                         LeftOrRightOrBaseline.RIGHT,
                         this.baselineEnsembleSeries.size(),
                         LeftOrRightOrBaseline.BASELINE );
        }
    }

}

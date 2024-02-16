package wres.datamodel.time;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.Ensemble;
import wres.datamodel.space.Feature;

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

    public Stream<TimeSeries<Double>> getSingleValuedSeries( DatasetOrientation orientation,
                                                             Set<Feature> features )
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

    public Stream<TimeSeries<Double>> getSingleValuedSeries( DatasetOrientation orientation )
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
                                                             DatasetOrientation orientation,
                                                             Set<Feature> features )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( orientation );
        Objects.requireNonNull( features );

        return TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                     this.rightSingleValuedSeries,
                                                     this.baselineSingleValuedSeries,
                                                     orientation )
                              .stream()
                              .filter( next -> features.contains( next.getMetadata()
                                                                      .getFeature() ) )
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
                                                           DatasetOrientation orientation,
                                                           Set<Feature> features )
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

    public Stream<TimeSeries<Ensemble>> getEnsembleSeries( DatasetOrientation orientation )
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

        public Builder addSingleValuedSeries( TimeSeries<Double> series, DatasetOrientation context )
        {
            Objects.requireNonNull( series );
            Objects.requireNonNull( context );

            TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                  this.rightSingleValuedSeries,
                                                  this.baselineSingleValuedSeries,
                                                  context )
                           .add( series );

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

        public Builder addEnsembleSeries( TimeSeries<Ensemble> series, DatasetOrientation context )
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

    private static Collection<TimeSeries<Double>> getSingleValuedStore( Collection<TimeSeries<Double>> leftSingleValuedSeries,
                                                                        Collection<TimeSeries<Double>> rightSingleValuedSeries,
                                                                        Collection<TimeSeries<Double>> baselineSingleValuedSeries,
                                                                        DatasetOrientation orientation )
    {
        return switch ( orientation )
        {
            case LEFT -> leftSingleValuedSeries;
            case RIGHT -> rightSingleValuedSeries;
            case BASELINE -> baselineSingleValuedSeries;
        };
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

    private static Collection<TimeSeries<Ensemble>> getEnsembleStore( Collection<TimeSeries<Ensemble>> leftEnsembleSeries,
                                                                      Collection<TimeSeries<Ensemble>> rightEnsembleSeries,
                                                                      Collection<TimeSeries<Ensemble>> baselineEnsembleSeries,
                                                                      DatasetOrientation context )
    {
        return switch ( context )
        {
            case LEFT -> leftEnsembleSeries;
            case RIGHT -> rightEnsembleSeries;
            case BASELINE -> baselineEnsembleSeries;
        };
    }

    /**
     * Creates an instance.
     * @param builder the builder
     */

    private TimeSeriesStore( Builder builder )
    {
        // No longer need a concurrent collection type because writing is complete and the collection is now immutable
        this.leftSingleValuedSeries = List.copyOf( builder.leftSingleValuedSeries );
        this.rightSingleValuedSeries =
                List.copyOf( builder.rightSingleValuedSeries );
        this.baselineSingleValuedSeries =
                List.copyOf( builder.baselineSingleValuedSeries );
        this.leftEnsembleSeries = List.copyOf( builder.leftEnsembleSeries );
        this.rightEnsembleSeries = List.copyOf( builder.rightEnsembleSeries );
        this.baselineEnsembleSeries = List.copyOf( builder.baselineEnsembleSeries );

        if ( LOGGER.isInfoEnabled() )
        {
            int size = this.leftSingleValuedSeries.size()
                       + this.rightSingleValuedSeries.size()
                       + this.baselineSingleValuedSeries.size()
                       + this.leftEnsembleSeries.size()
                       + this.rightEnsembleSeries.size()
                       + this.baselineEnsembleSeries.size();

            LOGGER.info( "Created an in-memory time-series store that contains {} time-series. The {} time-series "
                         + "include {} single-valued time-series with an {} orientation, {} single-valued time-series "
                         + "with a {} orientation, {} single-valued time-series with a {} orientation, {} ensemble "
                         + "time-series with an {} orientation, {} ensemble time-series with a {} orientation and {} "
                         + "ensemble time-series with a {} orientation. The total number of time-series events within "
                         + "the store is: {}.",
                         size,
                         size,
                         this.leftSingleValuedSeries.size(),
                         DatasetOrientation.LEFT,
                         this.rightSingleValuedSeries.size(),
                         DatasetOrientation.RIGHT,
                         this.baselineSingleValuedSeries.size(),
                         DatasetOrientation.BASELINE,
                         this.leftEnsembleSeries.size(),
                         DatasetOrientation.LEFT,
                         this.rightEnsembleSeries.size(),
                         DatasetOrientation.RIGHT,
                         this.baselineEnsembleSeries.size(),
                         DatasetOrientation.BASELINE,
                         this.getEventCount() );
        }
    }

    /**
     * @return the number of time-series events across all time-series in the store
     */
    private long getEventCount()
    {
        return this.leftSingleValuedSeries.stream()
                                          .mapToLong( e -> e.getEvents()
                                                            .size() )
                                          .sum()
               + this.rightSingleValuedSeries.stream()
                                             .mapToLong( e -> e.getEvents()
                                                               .size() )
                                             .sum()
               + this.baselineSingleValuedSeries.stream()
                                                .mapToLong( e -> e.getEvents()
                                                                  .size() )
                                                .sum()
               + this.leftEnsembleSeries.stream()
                                        .mapToLong( e -> e.getEvents()
                                                          .size() )
                                        .sum()
               + this.rightEnsembleSeries.stream()
                                         .mapToLong( e -> e.getEvents()
                                                           .size() )
                                         .sum()
               + this.baselineEnsembleSeries.stream()
                                            .mapToLong( e -> e.getEvents()
                                                              .size() )
                                            .sum();
    }

}

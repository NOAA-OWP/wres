package wres.datamodel.time;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
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
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class TimeSeriesStore
{
    /** Logger. */
    private final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesStore.class );

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
     * @param context the context
     * @param feature the feature
     * @return the filtered series
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the context is unrecognized
     */

    public Stream<TimeSeries<Double>> getSingleValuedSeries( LeftOrRightOrBaseline context, FeatureKey feature )
    {
        Objects.requireNonNull( context );
        Objects.requireNonNull( feature );

        return TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                     this.rightSingleValuedSeries,
                                                     this.baselineSingleValuedSeries,
                                                     context )
                              .stream()
                              .filter( next -> feature.equals( next.getMetadata().getFeature() ) );
    }

    /**
     * Filters the single-valued series by time window and feature.
     * @param timeWindow the time window
     * @param context the context
     * @param feature the feature
     * @return the filtered series
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the context is unrecognized
     */

    public Stream<TimeSeries<Double>> getSingleValuedSeries( TimeWindowOuter timeWindow,
                                                             LeftOrRightOrBaseline context,
                                                             FeatureKey feature )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( context );
        Objects.requireNonNull( feature );

        return TimeSeriesStore.getSingleValuedStore( this.leftSingleValuedSeries,
                                                     this.rightSingleValuedSeries,
                                                     this.baselineSingleValuedSeries,
                                                     context )
                              .stream()
                              .filter( next -> feature.equals( next.getMetadata().getFeature() ) )
                              .map( next -> TimeSeriesSlicer.filter( next, timeWindow ) );
    }

    /**
     * Filters the ensemble series by time window and feature.
     * @param timeWindow the time window
     * @param context the context
     * @param feature the feature
     * @return the filtered series
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the context is unrecognized
     */

    public Stream<TimeSeries<Ensemble>> getEnsembleSeries( TimeWindowOuter timeWindow,
                                                           LeftOrRightOrBaseline context,
                                                           FeatureKey feature )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( context );
        Objects.requireNonNull( feature );

        return TimeSeriesStore.getEnsembleStore( this.leftEnsembleSeries,
                                                 this.rightEnsembleSeries,
                                                 this.baselineEnsembleSeries,
                                                 context )
                              .stream()
                              .filter( next -> feature.equals( next.getMetadata().getFeature() ) )
                              .map( next -> TimeSeriesSlicer.filter( next,
                                                                     timeWindow ) );
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
     * @param context the context
     * @return a single-valued store with the prescribed context
     * @throws IllegalArgumentException if the context is unrecognized
     */

    private static Collection<TimeSeries<Double>>
            getSingleValuedStore( Collection<TimeSeries<Double>> leftSingleValuedSeries,
                                  Collection<TimeSeries<Double>> rightSingleValuedSeries,
                                  Collection<TimeSeries<Double>> baselineSingleValuedSeries,
                                  LeftOrRightOrBaseline context )
    {
        switch ( context )
        {
            case LEFT:
                return leftSingleValuedSeries;
            case RIGHT:
                return rightSingleValuedSeries;
            case BASELINE:
                return baselineSingleValuedSeries;
            default:
                throw new IllegalArgumentException( "Unexpected context '" + context
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

            LOGGER.info( "Created a time-series store that contains {} single-valued time-series with designation {}, "
                         + "{} single-valued time-series with designation {}, {} single-valued time-series with "
                         + "designation {}, {} ensemble time-series with designation {}, {} ensemble time-series with "
                         + "designation {} and {} ensemble time-series with designation {}. There are {} time-series "
                         + "in the store.",
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
                         LeftOrRightOrBaseline.BASELINE,
                         size );
        }
    }

}

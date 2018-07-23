package wres.datamodel.inputs.pairs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.SafeTimeSeries;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesHelper;

/**
 * <p>A {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfSingleValuedPairs extends SingleValuedPairs implements TimeSeries<PairOfDoubles>
{

    /**
     * Instance of base class for a time-series of pairs.
     */

    private final SafeTimeSeries<PairOfDoubles> main;

    /**
     * Instance of base class for a baseline time-series of pairs.
     */

    private final SafeTimeSeries<PairOfDoubles> baseline;

    @Override
    public TimeSeriesOfSingleValuedPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();
        builder.addTimeSeriesData( baseline.getRawData() ).setMetadata( getMetadataForBaseline() );
        return builder.build();
    }

    @Override
    public Iterable<Event<PairOfDoubles>> timeIterator()
    {
        return main.timeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubles>> basisTimeIterator()
    {
        return main.basisTimeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubles>> durationIterator()
    {
        return main.durationIterator();
    }

    @Override
    public List<Instant> getBasisTimes()
    {
        return Collections.unmodifiableList( main.getBasisTimes() );
    }

    @Override
    public SortedSet<Duration> getDurations()
    {
        return Collections.unmodifiableSortedSet( main.getDurations() );
    }

    @Override
    public boolean hasMultipleTimeSeries()
    {
        return main.hasMultipleTimeSeries();
    }

    @Override
    public boolean isRegular()
    {
        return main.isRegular();
    }

    @Override
    public Duration getRegularDuration()
    {
        return main.getRegularDuration();
    }

    @Override
    public Instant getEarliestBasisTime()
    {
        return TimeSeriesHelper.getEarliestBasisTime( this.getBasisTimes() );
    }

    @Override
    public Instant getLatestBasisTime()
    {
        return TimeSeriesHelper.getLatestBasisTime( this.getBasisTimes() );
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this );
    }

    /**
     * A {@link PairedInputBuilder} to build the metric input.
     */

    public static class TimeSeriesOfSingleValuedPairsBuilder extends SingleValuedPairsBuilder
            implements TimeSeriesBuilder<PairOfDoubles>
    {

        /**
         * The raw data.
         */

        private List<Event<List<Event<PairOfDoubles>>>> data = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<Event<List<Event<PairOfDoubles>>>> baselineData = null;

        /**
         * Adds a list of atomic time-series to the builder for a baseline, each one stored against its basis time.
         * 
         * @param values the time-series, stored against their basis times
         * @return the builder
         */

        public TimeSeriesOfSingleValuedPairsBuilder
                addTimeSeriesDataForBaseline( List<Event<List<Event<PairOfDoubles>>>> values )
        {
            if ( Objects.nonNull( values ) )
            {
                if ( Objects.isNull( this.baselineData ) )
                {
                    this.baselineData = new ArrayList<>();
                }
                List<Event<List<Event<PairOfDoubles>>>> sorted = TimeSeriesHelper.sort( values );
                this.baselineData.addAll( sorted );
                this.addDataForBaseline( TimeSeriesHelper.unwrap( sorted ) );
            }
            return this;
        }

        /**
         * Adds a time-series to the builder, including any baseline.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws MetricInputException if the specified input is inconsistent with any existing input
         */

        public TimeSeriesOfSingleValuedPairsBuilder
                addTimeSeries( TimeSeriesOfSingleValuedPairs timeSeries )
        {
            for ( TimeSeries<PairOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                //Add the main data
                List<Event<PairOfDoubles>> nextSource = new ArrayList<>();
                for ( Event<PairOfDoubles> nextEvent : a.timeIterator() )
                {
                    nextSource.add( nextEvent );
                }
                this.addTimeSeriesData( Arrays.asList( Event.of( a.getEarliestBasisTime(), nextSource ) ) );
            }

            // Set the union of the current metadata and any previously added time-series
            List<Metadata> mainMeta = new ArrayList<>();
            mainMeta.add( timeSeries.getMetadata() );
            if ( Objects.nonNull( this.mainMeta ) )
            {
                mainMeta.add( this.mainMeta );
            }
            this.setMetadata( MetadataFactory.unionOf( mainMeta ) );

            //Add the baseline data if required
            if ( timeSeries.hasBaseline() )
            {
                this.addTimeSeriesForBaseline( timeSeries.getBaselineData() );
            }

            this.setClimatology( timeSeries.getClimatology() );
            return this;
        }

        /**
         * Adds a time-series to the builder as a baseline dataset only. Any data associated with the 
         * {@link TimeSeriesOfSingleValuedPairs#getBaselineData()} of the input is ignored.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws MetricInputException if the specified input is inconsistent with any existing input
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesForBaseline( TimeSeriesOfSingleValuedPairs timeSeries )
        {
            for ( TimeSeries<PairOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                List<Event<PairOfDoubles>> nextSource = new ArrayList<>();

                for ( Event<PairOfDoubles> nextEvent : a.timeIterator() )
                {
                    nextSource.add( nextEvent );
                }
                this.addTimeSeriesDataForBaseline( Arrays.asList( Event.of( a.getEarliestBasisTime(),
                                                                            nextSource ) ) );
            }

            // Set the union of the current metadata and any previously added time-series
            List<Metadata> baselineMeta = new ArrayList<>();

            // Metadata, as with data, is taken from the main input
            baselineMeta.add( timeSeries.getMetadata() );
            if ( Objects.nonNull( this.baselineMeta ) )
            {
                baselineMeta.add( this.baselineMeta );
            }
            this.setMetadataForBaseline( MetadataFactory.unionOf( baselineMeta ) );

            return this;
        }

        /**
         * Adds a list of atomic time-series to the builder, each one stored against its basis time.
         * 
         * @param values the time-series, stored against their basis times
         * @return the builder
         */

        public TimeSeriesOfSingleValuedPairsBuilder
                addTimeSeriesData( List<Event<List<Event<PairOfDoubles>>>> values )
        {
            List<Event<List<Event<PairOfDoubles>>>> sorted = TimeSeriesHelper.sort( values );
            this.data.addAll( sorted );
            this.addData( TimeSeriesHelper.unwrap( sorted ) );
            return this;
        }

        /**
         * Builds a time-series.
         * 
         * @return a time-series
         */

        public TimeSeriesOfSingleValuedPairs build()
        {
            return new TimeSeriesOfSingleValuedPairs( this );
        }

        /**
         * Adds an atomic time-series to the builder.
         * 
         * @param basisTime the basis time for the time-series
         * @param values the pairs of time-series values, ordered from earliest to latest
         * @return the builder
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesData( Instant basisTime,
                                                                       List<Event<PairOfDoubles>> values )
        {
            TimeSeriesBuilder.super.addTimeSeriesData( basisTime, values );
            return this;
        }

        /**
         * Adds an atomic time-series to the builder for a baseline.
         * 
         * @param basisTime the basis time for the time-series
         * @param values the pairs of time-series values, ordered from earliest to latest
         * @return the builder
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesDataForBaseline( Instant basisTime,
                                                                                  List<Event<PairOfDoubles>> values )
        {
            List<Event<List<Event<PairOfDoubles>>>> input = new ArrayList<>();
            input.add( Event.of( basisTime, values ) );
            return addTimeSeriesDataForBaseline( input );
        }

        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws MetricInputException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeries( TimeSeries<PairOfDoubles> timeSeries )
        {
            TimeSeriesBuilder.super.addTimeSeries( timeSeries );
            return this;
        }

        /**
         * Adds a time-series to the builder for a baseline dataset.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws MetricInputException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesForBaseline( TimeSeries<PairOfDoubles> timeSeries )
        {
            Objects.requireNonNull( timeSeries, "Specify non-null time-series input." );

            for ( TimeSeries<PairOfDoubles> next : timeSeries.basisTimeIterator() )
            {
                Instant basisTime = next.getEarliestBasisTime();
                List<Event<PairOfDoubles>> values = new ArrayList<>();
                next.timeIterator().forEach( values::add );
                this.addTimeSeriesDataForBaseline( basisTime, values );
            }

            return this;
        }


    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    TimeSeriesOfSingleValuedPairs( final TimeSeriesOfSingleValuedPairsBuilder b )
    {
        super( b );
        this.main = SafeTimeSeries.of( b.data );
        // Baseline data?
        if ( this.hasBaseline() )
        {
            this.baseline = SafeTimeSeries.of( b.baselineData );
        }
        else
        {
            this.baseline = null;
        }
    }

}

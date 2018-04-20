package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Immutable implementation of a possibly irregular time-series of verification pairs that comprise two single-valued, 
 * continuous numerical, variables.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
class SafeTimeSeriesOfSingleValuedPairs extends SafeSingleValuedPairs
        implements TimeSeriesOfSingleValuedPairs
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
        SafeTimeSeriesOfSingleValuedPairsBuilder builder = new SafeTimeSeriesOfSingleValuedPairsBuilder();
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
     * A {@link DefaultMetricInputBuilder} to build the metric input.
     */

    static class SafeTimeSeriesOfSingleValuedPairsBuilder extends SingleValuedPairsBuilder
            implements TimeSeriesOfSingleValuedPairsBuilder
    {

        /**
         * The raw data.
         */

        private List<Event<List<Event<PairOfDoubles>>>> data = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<Event<List<Event<PairOfDoubles>>>> baselineData = null;

        @Override
        public SafeTimeSeriesOfSingleValuedPairsBuilder
                addTimeSeriesData( List<Event<List<Event<PairOfDoubles>>>> values )
        {
            List<Event<List<Event<PairOfDoubles>>>> sorted = TimeSeriesHelper.sort( values );
            this.data.addAll( sorted );
            this.addData( TimeSeriesHelper.unwrap( sorted ) );
            return this;
        }

        @Override
        public SafeTimeSeriesOfSingleValuedPairsBuilder
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

        @Override
        public SafeTimeSeriesOfSingleValuedPairsBuilder
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
            MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
            List<Metadata> mainMeta = new ArrayList<>();
            mainMeta.add( timeSeries.getMetadata() );
            if ( Objects.nonNull( this.mainMeta ) )
            {
                mainMeta.add( this.mainMeta );
            }
            this.setMetadata( metaFac.unionOf( mainMeta ) );

            //Add the baseline data if required
            if ( timeSeries.hasBaseline() )
            {
                this.addTimeSeriesForBaseline( timeSeries.getBaselineData() );
            }

            this.setClimatology( timeSeries.getClimatology() );
            return this;
        }

        @Override
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
            MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
            List<Metadata> baselineMeta = new ArrayList<>();

            // Metadata, as with data, is taken from the main input
            baselineMeta.add( timeSeries.getMetadata() );
            if ( Objects.nonNull( this.baselineMeta ) )
            {
                baselineMeta.add( this.baselineMeta );
            }
            this.setMetadataForBaseline( metaFac.unionOf( baselineMeta ) );

            return this;
        }

        @Override
        public SafeTimeSeriesOfSingleValuedPairs build()
        {
            return new SafeTimeSeriesOfSingleValuedPairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SafeTimeSeriesOfSingleValuedPairs( final SafeTimeSeriesOfSingleValuedPairsBuilder b )
    {
        super( b );
        main = new SafeTimeSeries<>( b.data );
        this.baseline = this.hasBaseline() ? new SafeTimeSeries<>( b.baselineData ): null;
    }

}

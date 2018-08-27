package wres.datamodel.sampledata.pairs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.time.BasicTimeSeries;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesHelper;

/**
 * Immutable implementation of a possibly irregular {@link TimeSeries} of {@link EnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfEnsemblePairs extends EnsemblePairs implements TimeSeries<EnsemblePair>
{

    /**
     * Instance of base class for a time-series of pairs.
     */

    private final BasicTimeSeries<EnsemblePair> main;

    /**
     * Instance of base class for a time-series of baseline pairs.
     */

    private final BasicTimeSeries<EnsemblePair> baseline;

    @Override
    public TimeSeriesOfEnsemblePairs getBaselineData()
    {
        if ( Objects.isNull( this.getRawDataForBaseline() ) )
        {
            return null;
        }
        TimeSeriesOfEnsemblePairsBuilder builder = new TimeSeriesOfEnsemblePairsBuilder();
        builder.addTimeSeriesData( baseline.getRawData() ).setMetadata( getMetadataForBaseline() );
        return builder.build();
    }

    @Override
    public Iterable<Event<EnsemblePair>> timeIterator()
    {
        return main.timeIterator();
    }

    @Override
    public Iterable<TimeSeries<EnsemblePair>> basisTimeIterator()
    {
        return main.basisTimeIterator();
    }

    @Override
    public Iterable<TimeSeries<EnsemblePair>> durationIterator()
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
        return TimeSeriesHelper.getEarliestBasisTime( getBasisTimes() );
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
     * A builder to build the metric input.
     */

    public static class TimeSeriesOfEnsemblePairsBuilder extends EnsemblePairsBuilder
            implements TimeSeriesBuilder<EnsemblePair>
    {

        /**
         * The raw data.
         */

        private List<Event<List<Event<EnsemblePair>>>> data = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<Event<List<Event<EnsemblePair>>>> baselineData = null;

        /**
         * Adds an atomic time-series to the builder.
         * 
         * @param basisTime the basis time for the time-series
         * @param values the pairs of time-series values, ordered from earliest to latest
         * @return the builder
         */

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesData( Instant basisTime,
                                                                   List<Event<EnsemblePair>> values )
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

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesDataForBaseline( Instant basisTime,
                                                                              List<Event<EnsemblePair>> values )
        {
            List<Event<List<Event<EnsemblePair>>>> input = new ArrayList<>();
            input.add( Event.of( basisTime, values ) );
            return addTimeSeriesDataForBaseline( input );
        }

        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeries( TimeSeries<EnsemblePair> timeSeries )
        {
            TimeSeriesBuilder.super.addTimeSeries( timeSeries );
            return this;
        }

        /**
         * Adds a time-series to the builder for a baseline dataset.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfEnsemblePairsBuilder
                addTimeSeriesForBaseline( TimeSeries<EnsemblePair> timeSeries )
        {
            Objects.requireNonNull( timeSeries, "Specify non-null time-series input." );

            for ( TimeSeries<EnsemblePair> next : timeSeries.basisTimeIterator() )
            {
                Instant basisTime = next.getEarliestBasisTime();
                List<Event<EnsemblePair>> values = new ArrayList<>();
                next.timeIterator().forEach( values::add );
                this.addTimeSeriesDataForBaseline( basisTime, values );
            }

            return this;
        }

        /**
         * Adds a list of atomic time-series to the builder, each one stored against its basis time.
         * 
         * @param values the time-series, stored against their basis times
         * @return the builder
         */

        public TimeSeriesOfEnsemblePairsBuilder
                addTimeSeriesData( List<Event<List<Event<EnsemblePair>>>> values )
        {
            List<Event<List<Event<EnsemblePair>>>> sorted = TimeSeriesHelper.sort( values );
            this.data.addAll( sorted );
            addData( TimeSeriesHelper.unwrap( sorted ) );
            return this;
        }

        /**
         * Adds a list of atomic time-series to the builder for a baseline, each one stored against its basis time.
         * 
         * @param values the time-series, stored against their basis times
         * @return the builder
         */

        public TimeSeriesOfEnsemblePairsBuilder
                addTimeSeriesDataForBaseline( List<Event<List<Event<EnsemblePair>>>> values )
        {
            if ( Objects.nonNull( values ) )
            {
                if ( Objects.isNull( this.baselineData ) )
                {
                    this.baselineData = new ArrayList<>();
                }
                List<Event<List<Event<EnsemblePair>>>> sorted = TimeSeriesHelper.sort( values );
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
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         */

        public TimeSeriesOfEnsemblePairsBuilder
                addTimeSeries( TimeSeriesOfEnsemblePairs timeSeries )
        {
            for ( TimeSeries<EnsemblePair> a : timeSeries.basisTimeIterator() )
            {
                List<Event<EnsemblePair>> nextSource = new ArrayList<>();

                for ( Event<EnsemblePair> nextEvent : a.timeIterator() )
                {
                    nextSource.add( nextEvent );
                }
                this.addTimeSeriesData( Arrays.asList( Event.of( a.getEarliestBasisTime(), nextSource ) ) );
            }

            // Set the union of the current metadata and any previously added time-series
            List<SampleMetadata> mainMeta = new ArrayList<>();
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
         * {@link TimeSeriesOfEnsemblePairs#getBaselineData()} of the input is ignored.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         */

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesForBaseline( TimeSeriesOfEnsemblePairs timeSeries )
        {
            for ( TimeSeries<EnsemblePair> a : timeSeries.basisTimeIterator() )
            {
                List<Event<EnsemblePair>> nextSource = new ArrayList<>();

                for ( Event<EnsemblePair> nextEvent : a.timeIterator() )
                {
                    nextSource.add( nextEvent );
                }
                this.addTimeSeriesDataForBaseline( Arrays.asList( Event.of( a.getEarliestBasisTime(),
                                                                            nextSource ) ) );
            }

            // Set the union of the current metadata and any previously added time-series
            List<SampleMetadata> baselineMeta = new ArrayList<>();

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
         * Builds a time-series.
         * 
         * @return a time-series
         */

        @Override
        public TimeSeriesOfEnsemblePairs build()
        {
            return new TimeSeriesOfEnsemblePairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    TimeSeriesOfEnsemblePairs( final TimeSeriesOfEnsemblePairsBuilder b )
    {
        super( b );
        this.main = BasicTimeSeries.of( b.data );
        // Baseline data?
        if ( Objects.nonNull( b.baselineData ) )
        {
            this.baseline = BasicTimeSeries.of( b.baselineData );
        }
        else
        {
            this.baseline = null;
        }
    }

//    @Override
//    public Iterable<TimeSeries<PairOfDoubles>> ensembleTraceIterator()
//    {
//        //Construct an iterable view of the ensemble traces
//        //Start with the basis times
//        //Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> basisTimes = basisTimeIterator().iterator();       
//        class IterableTimeSeries implements Iterable<TimeSeries<PairOfDoubles>>
//        {
//            @Override
//            public Iterator<TimeSeries<PairOfDoubles>> iterator()
//            {
//                return new Iterator<TimeSeries<PairOfDoubles>>()
//                {
//                    int currentBasisTime = -1;
//                    int totalBasisTimes = bP.getBasisTimes().size() - 1; //currentBasisTime starts at -1
//                    int currentTrace = 0;
//                    int totalTraces = getData().get( 0 ).getItemTwo().length;
//                    TimeSeriesOfEnsemblePairs currentSeries;
//                    Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> iterator = basisTimeIterator().iterator();
//                    DataFactory dFac = DefaultDataFactory.getInstance();
//
//                    @Override
//                    public boolean hasNext()
//                    {
//                        return currentBasisTime < totalBasisTimes ? true : currentTrace < totalTraces;
//                    }
//
//                    @Override
//                    public TimeSeriesOfSingleValuedPairs next()
//                    {
//                        if ( currentBasisTime >= totalBasisTimes && currentTrace >= totalTraces )
//                        {
//                            throw new NoSuchElementException( "No more traces to iterate." );
//                        }
//                        if ( currentTrace == totalTraces || currentSeries == null )
//                        {
//                            currentBasisTime += 1;
//                            currentTrace = 0;
//                            currentSeries = (TimeSeriesOfEnsemblePairs) iterator.next();
//                        }
//                        //Build a single-valued time-series with the trace at index currentTrace
//                        SafeTimeSeriesOfSingleValuedPairsBuilder builder =
//                                new SafeTimeSeriesOfSingleValuedPairsBuilder();
//                        builder.setMetadata( getMetadata() );
//
//                        List<Event<PairOfDoubles>> input = new ArrayList<>();
//                        for ( Event<PairOfDoubleAndVectorOfDoubles> next : currentSeries.timeIterator() )
//                        {
//                            input.add( Event.of( next.getTime(), dFac.pairOf( next.getValue().getItemOne(),
//                                                                              next.getValue()
//                                                                                  .getItemTwo()[currentTrace] ) ) );
//                        }
//                        builder.addTimeSeriesData( currentSeries.getEarliestBasisTime(), input );
//                        currentTrace++;
//                        //Set the climatology
//                        builder.setClimatology( getClimatology() );
//                        //Return the time-series
//                        return builder.build();
//                    }
//
//                    @Override
//                    public void remove()
//                    {
//                        throw new UnsupportedOperationException( TimeSeriesHelper.UNSUPPORTED_MODIFICATION );
//                    }
//                };
//            }
//        }
//        return new IterableTimeSeries();
//    }


}

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
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Immutable implementation of a possibly irregular time-series of verification pairs in which the right value 
 * comprises an ensemble.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
class SafeTimeSeriesOfEnsemblePairs extends SafeEnsemblePairs
        implements TimeSeriesOfEnsemblePairs
{

    /**
     * Instance of base class for a time-series of pairs.
     */

    private final SafeTimeSeries<PairOfDoubleAndVectorOfDoubles> main;
    
    /**
     * Instance of base class for a time-series of baseline pairs.
     */

    private final SafeTimeSeries<PairOfDoubleAndVectorOfDoubles> baseline;

    @Override
    public TimeSeriesOfEnsemblePairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        SafeTimeSeriesOfEnsemblePairsBuilder builder = new SafeTimeSeriesOfEnsemblePairsBuilder();
        builder.addTimeSeriesData( baseline.getRawData() ).setMetadata( getMetadataForBaseline() );
        return builder.build();
    }

    @Override
    public Iterable<Event<PairOfDoubleAndVectorOfDoubles>> timeIterator()
    {
        return main.timeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>> basisTimeIterator()
    {
        return main.basisTimeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>> durationIterator()
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
     * A {@link DefaultMetricInputBuilder} to build the metric input.
     */

    static class SafeTimeSeriesOfEnsemblePairsBuilder extends EnsemblePairsBuilder
            implements TimeSeriesOfEnsemblePairsBuilder
    {

        /**
         * The raw data.
         */

        private List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> data = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> baselineData = null;

        @Override
        public SafeTimeSeriesOfEnsemblePairsBuilder
                addTimeSeriesData( List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> values )
        {
            List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> sorted = TimeSeriesHelper.sort( values );
            data.addAll( sorted );
            addData( TimeSeriesHelper.unwrap( sorted ) );
            return this;
        }

        @Override
        public SafeTimeSeriesOfEnsemblePairsBuilder
                addTimeSeriesDataForBaseline( List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> values )
        {
            if ( Objects.nonNull( values ) )
            {
                if ( Objects.isNull( this.baselineData ) )
                {
                    this.baselineData = new ArrayList<>();
                }
                List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> sorted = TimeSeriesHelper.sort( values );
                this.baselineData.addAll( sorted );
                this.addDataForBaseline( TimeSeriesHelper.unwrap( sorted ) );
            }
            return this;
        }

        @Override
        public SafeTimeSeriesOfEnsemblePairsBuilder
                addTimeSeries( TimeSeriesOfEnsemblePairs timeSeries )
        {
            for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                List<Event<PairOfDoubleAndVectorOfDoubles>> nextSource = new ArrayList<>();

                for ( Event<PairOfDoubleAndVectorOfDoubles> nextEvent : a.timeIterator() )
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
        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesForBaseline( TimeSeriesOfEnsemblePairs timeSeries )
        {
            for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                List<Event<PairOfDoubleAndVectorOfDoubles>> nextSource = new ArrayList<>();

                for ( Event<PairOfDoubleAndVectorOfDoubles> nextEvent : a.timeIterator() )
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
        public SafeTimeSeriesOfEnsemblePairs build()
        {
            return new SafeTimeSeriesOfEnsemblePairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SafeTimeSeriesOfEnsemblePairs( final SafeTimeSeriesOfEnsemblePairsBuilder b )
    {
        super( b );
        main = new SafeTimeSeries<>( b.data );
        this.baseline = this.hasBaseline() ? new SafeTimeSeries<>( b.baselineData ): null;
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

package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.Predicate;

import wres.datamodel.SafeRegularTimeSeriesOfSingleValuedPairs.SafeRegularTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.builders.RegularTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Immutable implementation of a regular time-series of verification pairs in which the right value comprises an 
 * ensemble.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
class SafeRegularTimeSeriesOfEnsemblePairs extends SafeEnsemblePairs
        implements TimeSeriesOfEnsemblePairs
{

    /**
     * Instance of base class for a regular time-series of pairs.
     */

    private final SafeRegularTimeSeriesOfPairs<PairOfDoubleAndVectorOfDoubles> bP;

    @Override
    public TimeSeriesOfEnsemblePairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        SafeRegularTimeSeriesOfEnsemblePairsBuilder builder = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        builder.setTimeStep( getRegularDuration() ).setMetadata( getMetadataForBaseline() );
        List<PairOfDoubleAndVectorOfDoubles> baselineData = getDataForBaseline();
        int start = 0;
        for ( Instant next : bP.getBasisTimesBaseline() )
        {
            builder.addTimeSeriesData( next, baselineData.subList( start, start + bP.getTimeStepCount() ) );
            start += bP.getTimeStepCount();
        }
        return builder.build();
    }

    @Override
    public Iterable<Event<PairOfDoubleAndVectorOfDoubles>> timeIterator()
    {
        return bP.timeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>> basisTimeIterator()
    {
        return bP.basisTimeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>> durationIterator()
    {
        return bP.durationIterator();
    }

    @Override
    public TimeSeries<PairOfDoubleAndVectorOfDoubles> filterByDuration( Predicate<Duration> duration )
    {
        Objects.requireNonNull( duration, "Provide a non-null predicate on which to filter by duration." );
        //Iterate through the durations and append to the builder
        //Throw an exception if attempting to construct an irregular time-series
        RegularTimeSeriesOfEnsemblePairsBuilder builder = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        Integer step = null;
        int sinceLast = 0;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : durationIterator() )
        {
            sinceLast++;
            TimeSeriesOfEnsemblePairs next = (TimeSeriesOfEnsemblePairs) a;
            if ( duration.test( a.getDurations().first() ) )
            {
                if ( Objects.isNull( step ) )
                {
                    step = sinceLast;
                }
                else if ( step != sinceLast )
                {
                    throw new UnsupportedOperationException( "The filtered view of durations attempted to build "
                                                             + "an irregular time-series, which is not supported." );
                }
                builder.addTimeSeries( next );
                sinceLast = 0;
            }
        }

        //Nothing to build
        if ( Objects.isNull( step ) )
        {
            return null;
        }
        //Set regular time-step of filtered data
        builder.setTimeStep( getRegularDuration().multipliedBy( step ) );

        //Build if something to build
        return builder.build();
    }

    @Override
    public TimeSeries<PairOfDoubleAndVectorOfDoubles> filterByBasisTime( Predicate<Instant> basisTime )
    {
        Objects.requireNonNull( basisTime, "Provide a non-null predicate on which to filter by basis time." );
        SafeRegularTimeSeriesOfEnsemblePairsBuilder builder = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        builder.setTimeStep( getRegularDuration() );
        //Add the filtered data
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : basisTimeIterator() )
        {
            if ( basisTime.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( (TimeSeriesOfEnsemblePairs) a );
            }
        }
        //Build if something to build
        if ( !builder.mainInput.isEmpty() )
        {
            return builder.build();
        }
        return null;
    }

    @Override
    public TimeSeriesOfEnsemblePairs filterByTraceIndex( Predicate<Integer> traceFilter )
    {
        //Build a single-valued time-series with the trace at index currentTrace
        SafeRegularTimeSeriesOfEnsemblePairsBuilder builder =
                new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        builder.setTimeStep( getRegularDuration() );
        builder.setMetadata( getMetadata() );
        DataFactory dFac = DefaultDataFactory.getInstance();
        //Iterate through the basis times
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> nextSeries : basisTimeIterator() )
        {
            List<PairOfDoubleAndVectorOfDoubles> input = new ArrayList<>();
            //Iterate through the pairs
            for ( PairOfDoubleAndVectorOfDoubles next : (TimeSeriesOfEnsemblePairs) nextSeries )
            {
                //Reform the pairs with a subset of ensemble members
                double[] allTraces = next.getItemTwo();
                List<Double> subTraces = new ArrayList<>();
                for ( int i = 0; i < allTraces.length; i++ )
                {
                    if ( traceFilter.test( i ) )
                    {
                        subTraces.add( allTraces[i] );
                    }
                }
                //All time-series have the same number of ensemble members, 
                //so the first instance with no members means no traces
                if ( subTraces.isEmpty() )
                {
                    return null;
                }
                input.add( dFac.pairOf( next.getItemOne(), subTraces.toArray( new Double[subTraces.size()] ) ) );
            }
            builder.addTimeSeriesData( nextSeries.getEarliestBasisTime(), input );
        }
        //Return the time-series
        return builder.build();
    }

    @Override
    public List<Instant> getBasisTimes()
    {
        return Collections.unmodifiableList( bP.getBasisTimes() );
    }   

    @Override
    public SortedSet<Duration> getDurations()
    {
        return bP.getDurations();
    }

    @Override
    public boolean hasMultipleTimeSeries()
    {
        return bP.hasMultipleTimeSeries();
    }

    @Override
    public boolean isRegular()
    {
        return true;
    }

    @Override
    public Duration getRegularDuration()
    {
        return bP.getRegularDuration();
    }

    @Override
    public Instant getEarliestBasisTime()
    {
        return TimeSeriesHelper.getEarliestBasisTime( getBasisTimes() );
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this );
    }

    /**
     * A {@link DefaultPairedInputBuilder} to build the metric input.
     */

    static class SafeRegularTimeSeriesOfEnsemblePairsBuilder extends EnsemblePairsBuilder
            implements RegularTimeSeriesOfEnsemblePairsBuilder
    {

        /**
         * A list of basis times. There are as many basis times as atomic time-series.
         */

        private final List<Instant> basisTimes = new ArrayList<>();

        /**
         * A list of basis times associated with a baseline dataset. There are as many basis times as atomic 
         * time-series.
         */

        private final List<Instant> basisTimesBaseline = new ArrayList<>();

        /**
         * The time-step associated with the regular time-series.
         */

        private Duration timeStep;

        /**
         * The number of elements in each time-series added. This is used for validation.
         */

        private List<Integer> timeStepCount = new ArrayList<>();

        /**
         * The number of elements in each baseline time-series added. This is used for validation.
         */

        private List<Integer> timeStepCountBaseline = new ArrayList<>();

        @Override
        public SafeRegularTimeSeriesOfEnsemblePairsBuilder
                addTimeSeriesData( List<Event<List<PairOfDoubleAndVectorOfDoubles>>> values )
        {
            Objects.requireNonNull( values, "Enter non-null data for the time-series." );
            for ( Event<List<PairOfDoubleAndVectorOfDoubles>> next : values )
            {
                addData( next.getValue() );
                timeStepCount.add( next.getValue().size() );
                basisTimes.add( next.getTime() );
            }
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfEnsemblePairsBuilder
                addTimeSeriesDataForBaseline( List<Event<List<PairOfDoubleAndVectorOfDoubles>>> values )
        {
            Objects.requireNonNull( values, "Enter non-null data for the baseline time-series." );
            for ( Event<List<PairOfDoubleAndVectorOfDoubles>> next : values )
            {
                addDataForBaseline( next.getValue() );
                timeStepCountBaseline.add( next.getValue().size() );
                basisTimesBaseline.add( next.getTime() );
            }
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfEnsemblePairsBuilder
                addTimeSeries( TimeSeriesOfEnsemblePairs timeSeries )
        {
            //Validate where possible
            if ( !timeSeries.isRegular() )
            {
                throw new MetricInputException( "Cannot add an irregular time-series to a container of regular "
                                                + "time-series." );
            }
            if ( Objects.nonNull( timeStep ) && !timeSeries.getRegularDuration().equals( timeStep ) )
            {
                throw new MetricInputException( "The input time-series has a time-step of '"
                                                + timeSeries.getRegularDuration()
                                                + "', which is inconsistent with the time-step of the existing data, '"
                                                + timeStep
                                                + "'." );
            }
            setTimeStep( timeSeries.getRegularDuration() );
            List<Metadata> mainMeta = new ArrayList<>();
            List<Metadata> baselineMeta = new ArrayList<>();
            VectorOfDoubles climatology = null;
            MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
            //Add the main data
            for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                TimeSeriesOfEnsemblePairs next = (TimeSeriesOfEnsemblePairs) a;
                addTimeSeriesData( next.getEarliestBasisTime(), next.getData() );
                mainMeta.add( next.getMetadata() );
                //Add climatology if available
                if( next.hasClimatology() )
                {
                    climatology = next.getClimatology();
                }
            }
            setMetadata( metaFac.unionOf( mainMeta ) );
            
            //Add any baseline data
            if( timeSeries.hasBaseline() )
            {
                for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : timeSeries.getBaselineData().basisTimeIterator() )
                {
                    //Add the main data
                    TimeSeriesOfEnsemblePairs next = (TimeSeriesOfEnsemblePairs) a;
                    addTimeSeriesDataForBaseline( next.getEarliestBasisTime(), next.getData() );
                    baselineMeta.add( next.getMetadata() );
                }
                setMetadataForBaseline( metaFac.unionOf( baselineMeta ) );
            }   
            setClimatology( climatology );
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfEnsemblePairsBuilder setTimeStep( Duration timeStep )
        {
            this.timeStep = timeStep;
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfEnsemblePairs build()
        {
            return new SafeRegularTimeSeriesOfEnsemblePairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SafeRegularTimeSeriesOfEnsemblePairs( final SafeRegularTimeSeriesOfEnsemblePairsBuilder b )
    {
        super( b );
        //Check that all pairs have the same number of ensemble members
        int count = 0;
        for ( PairOfDoubleAndVectorOfDoubles next : getData() )
        {
            if ( count == 0 )
            {
                count = next.getItemTwo().length;
            }
            else if ( next.getItemTwo().length != count )
            {
                throw new MetricInputException( "While building a regular time-series of ensemble pairs: each pair "
                                                + "must contain the same number of ensemble members." );
            }
        }
        //Baseline can differ from main, but must have a constant number of ensemble members
        if ( hasBaseline() )
        {
            int baselineCount = 0;

            for ( PairOfDoubleAndVectorOfDoubles next : getDataForBaseline() )
            {
                if ( baselineCount == 0 )
                {
                    baselineCount = next.getItemTwo().length;
                }
                else if ( next.getItemTwo().length != baselineCount )
                {
                    throw new MetricInputException( "While building a regular time-series of ensemble pairs: each pair "
                                                    + "in the baseline must contain the same number of ensemble "
                                                    + "members." );
                }
            }
        }
        bP = new SafeRegularTimeSeriesOfPairs<>( getData(),
                                                 b.timeStep,
                                                 b.basisTimes,
                                                 b.basisTimesBaseline,
                                                 b.timeStepCount,
                                                 b.timeStepCountBaseline,
                                                 getBasisTimeIterator(),
                                                 getDurationIterator() );

    }

    /**
     * Returns an {@link Iterable} view of the atomic time-series by basis time.
     * 
     * @return an iterable view of the basis times
     */

    private Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>> getBasisTimeIterator()
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>>
        {
            @Override
            public Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> iterator()
            {
                return new Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>>()
                {
                    int returned = 0;
                    Iterator<Instant> iterator = getBasisTimes().iterator();
                    List<PairOfDoubleAndVectorOfDoubles> data = getData();

                    @Override
                    public boolean hasNext()
                    {
                        return returned < getBasisTimes().size();
                    }

                    @Override
                    public TimeSeries<PairOfDoubleAndVectorOfDoubles> next()
                    {
                        if ( returned >= getBasisTimes().size() )
                        {
                            throw new NoSuchElementException( "No more basis times to iterate." );
                        }
                        SafeRegularTimeSeriesOfEnsemblePairsBuilder builder =
                                new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
                        Instant nextTime = iterator.next();
                        int start = returned * bP.getTimeStepCount();
                        builder.setTimeStep( getRegularDuration() );
                        builder.addTimeSeriesData( nextTime, data.subList( start, start + bP.getTimeStepCount() ) );
                        //Adjust the time window for the metadata
                        builder.setMetadata( TimeSeriesHelper.getBasisTimeAdjustedMetadata( getMetadata(),
                                                                                                        nextTime,
                                                                                                        nextTime ) );
                        // Set the climatology
                        builder.setClimatology( getClimatology() );
                        returned++;
                        return builder.build();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( TimeSeriesHelper.UNSUPPORTED_MODIFICATION );
                    }
                };
            }
        }
        return new IterableTimeSeries();
    }

    /**
     * Returns an {@link Iterable} view of the atomic time-series by duration.
     * 
     * @return an iterable view of the durations
     */

    private Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>> getDurationIterator()
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<PairOfDoubleAndVectorOfDoubles>>
        {
            @Override
            public Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> iterator()
            {
                return new Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>>()
                {
                    int returned = 0;
                    List<PairOfDoubleAndVectorOfDoubles> data = getData();

                    @Override
                    public boolean hasNext()
                    {
                        return returned < bP.getTimeStepCount();
                    }

                    @Override
                    public TimeSeries<PairOfDoubleAndVectorOfDoubles> next()
                    {
                        if ( returned >= bP.getTimeStepCount() )
                        {
                            throw new NoSuchElementException( "No more durations to iterate." );
                        }
                        SafeRegularTimeSeriesOfEnsemblePairsBuilder builder =
                                new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
                        Duration nextDuration = getRegularDuration().multipliedBy( (long) returned + 1 );
                        builder.setTimeStep( nextDuration );
                        //Adjust the time window for the metadata
                        builder.setMetadata( TimeSeriesHelper.getDurationAdjustedMetadata( getMetadata(),
                                                                                                       nextDuration,
                                                                                                       nextDuration ) );
                        int start = 0;
                        for ( Instant next : bP.getBasisTimes() )
                        {
                            List<PairOfDoubleAndVectorOfDoubles> subset = new ArrayList<>();
                            subset.add( data.get( start + returned ) );
                            builder.addTimeSeriesData( next, subset );
                            start += bP.getTimeStepCount();
                        }
                        // Set the climatology
                        builder.setClimatology( getClimatology() );
                        returned++;
                        return builder.build();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( TimeSeriesHelper.UNSUPPORTED_MODIFICATION );
                    }
                };
            }
        }
        return new IterableTimeSeries();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubles>> ensembleTraceIterator()
    {
        //Construct an iterable view of the ensemble traces
        //Start with the basis times
        //Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> basisTimes = basisTimeIterator().iterator();       
        class IterableTimeSeries implements Iterable<TimeSeries<PairOfDoubles>>
        {
            @Override
            public Iterator<TimeSeries<PairOfDoubles>> iterator()
            {
                return new Iterator<TimeSeries<PairOfDoubles>>()
                {
                    int currentBasisTime = -1;
                    int totalBasisTimes = bP.getBasisTimes().size() - 1; //currentBasisTime starts at -1
                    int currentTrace = 0;
                    int totalTraces = getData().get( 0 ).getItemTwo().length;
                    TimeSeriesOfEnsemblePairs currentSeries;
                    Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> iterator = basisTimeIterator().iterator();
                    DataFactory dFac = DefaultDataFactory.getInstance();

                    @Override
                    public boolean hasNext()
                    {
                        return currentBasisTime < totalBasisTimes ? true: currentTrace < totalTraces;
                    }

                    @Override
                    public TimeSeriesOfSingleValuedPairs next()
                    {
                        if ( currentBasisTime >= totalBasisTimes && currentTrace >= totalTraces )
                        {
                            throw new NoSuchElementException( "No more traces to iterate." );
                        }
                        if ( currentTrace == totalTraces || currentSeries == null )
                        {
                            currentBasisTime += 1;
                            currentTrace = 0;
                            currentSeries = (TimeSeriesOfEnsemblePairs) iterator.next();
                        }
                        //Build a single-valued time-series with the trace at index currentTrace
                        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder =
                                new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
                        builder.setTimeStep( getRegularDuration() );
                        builder.setMetadata( getMetadata() );

                        List<PairOfDoubles> input = new ArrayList<>();
                        for ( PairOfDoubleAndVectorOfDoubles next : currentSeries )
                        {
                            input.add( dFac.pairOf( next.getItemOne(),
                                                    next.getItemTwo()[currentTrace] ) );
                        }
                        builder.addTimeSeriesData( currentSeries.getEarliestBasisTime(), input );
                        currentTrace++;
                        //Set the climatology
                        builder.setClimatology( getClimatology() );
                        //Return the time-series
                        return builder.build();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( TimeSeriesHelper.UNSUPPORTED_MODIFICATION );
                    }
                };
            }
        }
        return new IterableTimeSeries();
    }

}

package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.builders.RegularTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.time.TimeSeries;

/**
 * Immutable implementation of a regular time-series of verification pairs that comprise two single-valued, continuous 
 * numerical, variables.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
class SafeRegularTimeSeriesOfSingleValuedPairs extends SafeSingleValuedPairs
        implements TimeSeriesOfSingleValuedPairs
{

    /**
     * Base class for a regular time-series of pairs.
     */

    private final SafeRegularTimeSeriesOfPairs<PairOfDoubles> bP;

    @Override
    public TimeSeriesOfSingleValuedPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        builder.setTimeStep( getRegularDuration() ).setMetadata( getMetadataForBaseline() );
        List<PairOfDoubles> baselineData = getDataForBaseline();
        int start = 0;
        for ( Instant next : bP.getBasisTimesBaseline() )
        {
            builder.addData( next, baselineData.subList( start, start + bP.getTimeStepCount() ) );
            start += bP.getTimeStepCount();
        }
        return builder.build();
    }

    @Override
    public Iterable<Pair<Instant, PairOfDoubles>> timeIterator()
    {
        return bP.timeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubles>> basisTimeIterator()
    {
        return bP.basisTimeIterator();
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubles>> durationIterator()
    {
        return bP.durationIterator();
    }

    @Override
    public TimeSeries<PairOfDoubles> filterByDuration( Predicate<Duration> duration )
    {
        Objects.requireNonNull( duration, "Provide a non-null predicate on which to filter by duration." );
        //Iterate through the durations and append to the builder
        //Throw an exception if attempting to construct an irregular time-series
        RegularTimeSeriesOfSingleValuedPairsBuilder builder = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        Integer step = null;
        int sinceLast = 0;
        //Time windows of the atomic time-series from which a union will be formed
        List<TimeWindow> windows = new ArrayList<>();
        List<TimeWindow> baselineWindows = new ArrayList<>();
        for ( TimeSeries<PairOfDoubles> a : durationIterator() )
        {
            sinceLast++;
            TimeSeriesOfSingleValuedPairs next = (TimeSeriesOfSingleValuedPairs) a;
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
                //Add the windows
                if ( getMetadata().hasTimeWindow() )
                {
                    windows.add( next.getMetadata().getTimeWindow() );
                }
                if ( hasBaseline() && getMetadataForBaseline().hasTimeWindow() )
                {
                    baselineWindows.add( next.getMetadataForBaseline().getTimeWindow() );
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
        //Set the new metadata with the union of the time windows if required
        builder.setMetadata( SafeRegularTimeSeriesOfPairs.getMetadataWithUnionOfTimeWindows( getMetadata(), windows ) );
        builder.setMetadataForBaseline( SafeRegularTimeSeriesOfPairs.getMetadataWithUnionOfTimeWindows( getMetadataForBaseline(),
                                                                                                        baselineWindows ) );

        //Build if something to build
        return builder.build();
    }

    @Override
    public TimeSeries<PairOfDoubles> filterByBasisTime( Predicate<Instant> basisTime )
    {
        Objects.requireNonNull( basisTime, "Provide a non-null predicate on which to filter by basis time." );
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        builder.setTimeStep( getRegularDuration() );
        //Add the filtered data
        for ( TimeSeries<PairOfDoubles> a : basisTimeIterator() )
        {
            if ( basisTime.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( (TimeSeriesOfSingleValuedPairs) a );
            }
        }
        //Build if something to build
        if ( Objects.nonNull( builder.mainInput ) )
        {
            return builder.build();
        }
        return null;
    }

    @Override
    public List<Instant> getBasisTimes()
    {
        return new ArrayList<>( bP.getBasisTimes() );
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
        return bP.getEarliestBasisTime();
    }

    @Override
    public String toString()
    {
        return bP.toString();
    }

    /**
     * A {@link DefaultPairedInputBuilder} to build the metric input.
     */

    static class SafeRegularTimeSeriesOfSingleValuedPairsBuilder extends SingleValuedPairsBuilder
            implements RegularTimeSeriesOfSingleValuedPairsBuilder
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
        public SafeRegularTimeSeriesOfSingleValuedPairsBuilder addData( Map<Instant, List<PairOfDoubles>> values )
        {
            Objects.requireNonNull( values, "Enter non-null data for the time-series." );
            for ( Entry<Instant, List<PairOfDoubles>> next : values.entrySet() )
            {
                addData( next.getValue() );
                timeStepCount.add( next.getValue().size() );
                basisTimes.add( next.getKey() );
            }
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfSingleValuedPairsBuilder
                addDataForBaseline( Map<Instant, List<PairOfDoubles>> values )
        {
            Objects.requireNonNull( values, "Enter non-null data for the time-series." );
            for ( Entry<Instant, List<PairOfDoubles>> next : values.entrySet() )
            {
                addDataForBaseline( next.getValue() );
                timeStepCountBaseline.add( next.getValue().size() );
                basisTimesBaseline.add( next.getKey() );
            }
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfSingleValuedPairsBuilder
                addTimeSeries( TimeSeriesOfSingleValuedPairs timeSeries )
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
            for ( TimeSeries<PairOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                //Add the main data
                TimeSeriesOfSingleValuedPairs next = (TimeSeriesOfSingleValuedPairs) a;
                addData( next.getEarliestBasisTime(), next.getData() );
                setMetadata( next.getMetadata() );
                //Add the baseline data if required
                if ( next.hasBaseline() )
                {
                    addDataForBaseline( next.getEarliestBasisTime(), next.getDataForBaseline() );
                    setMetadataForBaseline( next.getMetadataForBaseline() );
                }
            }
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfSingleValuedPairsBuilder setTimeStep( Duration timeStep )
        {
            this.timeStep = timeStep;
            return this;
        }

        @Override
        public SafeRegularTimeSeriesOfSingleValuedPairs build()
        {
            return new SafeRegularTimeSeriesOfSingleValuedPairs( this );
        }
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SafeRegularTimeSeriesOfSingleValuedPairs( final SafeRegularTimeSeriesOfSingleValuedPairsBuilder b )
    {
        super( b );
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

    private Iterable<TimeSeries<PairOfDoubles>> getBasisTimeIterator()
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<PairOfDoubles>>
        {
            @Override
            public Iterator<TimeSeries<PairOfDoubles>> iterator()
            {
                return new Iterator<TimeSeries<PairOfDoubles>>()
                {
                    int returned = 0;
                    Iterator<Instant> iterator = getBasisTimes().iterator();
                    List<PairOfDoubles> data = getData();
                    List<PairOfDoubles> baselineData = getDataForBaseline();

                    @Override
                    public boolean hasNext()
                    {
                        return returned < getBasisTimes().size();
                    }

                    @Override
                    public TimeSeries<PairOfDoubles> next()
                    {
                        if ( returned >= getBasisTimes().size() )
                        {
                            throw new NoSuchElementException( "No more basis times to iterate." );
                        }
                        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder =
                                new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
                        Instant nextTime = iterator.next();
                        int start = returned * bP.getTimeStepCount();
                        builder.setTimeStep( getRegularDuration() );
                        builder.addData( nextTime, data.subList( start, start + bP.getTimeStepCount() ) );
                        //Adjust the time window for the metadata
                        builder.setMetadata( SafeRegularTimeSeriesOfPairs.getBasisTimeAdjustedMetadata( getMetadata(),
                                                                                                        nextTime,
                                                                                                        nextTime ) );
                        //Add the baseline, if available for this time
                        if ( hasBaseline() && bP.getBasisTimesBaseline().contains( nextTime ) )
                        {
                            int startBase = bP.getBasisTimesBaseline().indexOf( nextTime ) * bP.getTimeStepCount();
                            builder.addDataForBaseline( nextTime,
                                                        baselineData.subList( startBase,
                                                                              startBase + bP.getTimeStepCount() ) );
                            //Adjust the time window for the metadata
                            builder.setMetadataForBaseline( SafeRegularTimeSeriesOfPairs.getBasisTimeAdjustedMetadata( getMetadataForBaseline(),
                                                                                                                       nextTime,
                                                                                                                       nextTime ) );
                        }
                        returned++;
                        return builder.build();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( SafeRegularTimeSeriesOfPairs.UNSUPPORTED_MODIFICATION );
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

    private Iterable<TimeSeries<PairOfDoubles>> getDurationIterator()
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<PairOfDoubles>>
        {
            @Override
            public Iterator<TimeSeries<PairOfDoubles>> iterator()
            {
                return new Iterator<TimeSeries<PairOfDoubles>>()
                {
                    int returned = 0;
                    List<PairOfDoubles> data = getData();
                    List<PairOfDoubles> baselineData = getDataForBaseline();

                    @Override
                    public boolean hasNext()
                    {
                        return returned < bP.getTimeStepCount();
                    }

                    @Override
                    public TimeSeries<PairOfDoubles> next()
                    {
                        if ( returned >= bP.getTimeStepCount() )
                        {
                            throw new NoSuchElementException( "No more durations to iterate." );
                        }
                        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder =
                                new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
                        Duration nextDuration = getRegularDuration().multipliedBy( (long) returned + 1 );
                        builder.setTimeStep( nextDuration );
                        //Adjust the time window for the metadata
                        builder.setMetadata( SafeRegularTimeSeriesOfPairs.getDurationAdjustedMetadata( getMetadata(),
                                                                                                       nextDuration,
                                                                                                       nextDuration ) );
                        int start = 0;
                        for ( Instant next : bP.getBasisTimes() )
                        {
                            List<PairOfDoubles> subset = new ArrayList<>();
                            subset.add( data.get( start + returned ) );
                            builder.addData( next, subset );
                            start += bP.getTimeStepCount();
                        }
                        //Add filtered baseline data
                        if ( hasBaseline() )
                        {
                            start = 0; //Reset counter
                            for ( Instant next : bP.getBasisTimesBaseline() )
                            {
                                List<PairOfDoubles> subset = new ArrayList<>();
                                subset.add( baselineData.get( start + returned ) );
                                builder.addDataForBaseline( next, subset );
                                start += bP.getTimeStepCount();
                            }
                            //Adjust the time window for the metadata
                            builder.setMetadataForBaseline( SafeRegularTimeSeriesOfPairs.getDurationAdjustedMetadata( getMetadataForBaseline(),
                                                                                                                      nextDuration,
                                                                                                                      nextDuration ) );
                        }
                        returned++;
                        return builder.build();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( SafeRegularTimeSeriesOfPairs.UNSUPPORTED_MODIFICATION );
                    }
                };
            }
        }
        return new IterableTimeSeries();
    }

}

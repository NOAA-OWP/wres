package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Predicate;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.Pair;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.RegularTimeSeriesOfSingleValuedPairs;
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
        implements RegularTimeSeriesOfSingleValuedPairs
{

    /**
     * The time-step associated with the regular time-series.
     */

    private final Duration timeStep;

    /**
     * A set of basis times. There are as many basis times as atomic time-series.
     */

    private final List<Instant> basisTimes;

    /**
     * A set of basis times associated with a baseline dataset. There are as many basis times as atomic time-series.
     */

    private final List<Instant> basisTimesBaseline;

    /**
     * The number of times in each atomic time-series.
     */

    private final int timeStepCount;

    /**
     * Iterable view of the basis times.
     */

    private final Iterable<TimeSeries<PairOfDoubles>> basisTimeIterator;

    /**
     * Iterable view of the lead times.
     */

    private final Iterable<TimeSeries<PairOfDoubles>> leadTimeIterator;

    /**
     * Iterable view of the pairs of times and values. 
     */

    private final Iterable<Pair<Instant, PairOfDoubles>> timeIterator;

    /**
     * Error message denoting attempt to modify an immutable time-series via an iterator.
     */

    private static final String UNSUPPORTED_MODIFICATION = " While attempting to modify an immutable time-series.";

    @Override
    public RegularTimeSeriesOfSingleValuedPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        builder.setTimeStep( timeStep ).setMetadata( getMetadataForBaseline() );
        List<PairOfDoubles> baselineData = getDataForBaseline();
        int start = 0;
        for ( Instant next : basisTimesBaseline )
        {
            builder.addData( next, baselineData.subList( start, start + timeStepCount ) );
            start += timeStepCount;
        }
        return builder.build();
    }

    @Override
    public Iterable<Pair<Instant, PairOfDoubles>> timeIterator()
    {
        return timeIterator;
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubles>> basisTimeIterator()
    {
        return basisTimeIterator;
    }

    @Override
    public Iterable<TimeSeries<PairOfDoubles>> durationIterator()
    {
        return leadTimeIterator;
    }

    @Override
    public TimeSeries<PairOfDoubles> filterByDuration( Predicate<Duration> duration )
    {
        Objects.requireNonNull( duration, "Provide a non-null predicate on which to filter lead time." );
        //Iterate through the lead times and append to the builder
        //Throw an exception if attempting to construct an irregular time-series
        RegularTimeSeriesOfSingleValuedPairsBuilder builder = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        Integer step = null;
        int sinceLast = 0;
        for ( TimeSeries<PairOfDoubles> a : durationIterator() )
        {
            sinceLast++;
            RegularTimeSeriesOfSingleValuedPairs next = (RegularTimeSeriesOfSingleValuedPairs) a;
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
        builder.setTimeStep( timeStep.multipliedBy( step ) );

        //Build if something to build
        return builder.build();
    }

    @Override
    public TimeSeries<PairOfDoubles> filterByBasisTime( Predicate<Instant> basisTime )
    {
        Objects.requireNonNull( basisTime, "Provide a non-null predicate on which to filter basis time." );
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        builder.setTimeStep( timeStep );
        //Add the filtered data
        for ( TimeSeries<PairOfDoubles> a : basisTimeIterator() )
        {
            if ( basisTime.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( (RegularTimeSeriesOfSingleValuedPairs) a );
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
    public SortedSet<Instant> getBasisTimes()
    {
        return new TreeSet<>( basisTimes );
    }

    @Override
    public SortedSet<Duration> getDurations()
    {
        SortedSet<Duration> returnMe = new TreeSet<>();
        for ( long i = 0; i < timeStepCount; i++ )
        {
            returnMe.add( timeStep.multipliedBy( i + 1 ) );
        }
        return returnMe;
    }

    @Override
    public boolean hasMultipleTimeSeries()
    {
        return basisTimes.size() > 1;
    }

    @Override
    public boolean isRegular()
    {
        return true;
    }

    @Override
    public Duration getRegularDuration()
    {
        return timeStep;
    }

    @Override
    public Instant getEarliestBasisTime()
    {
        if ( basisTimes.size() == 1 )
        {
            return ( basisTimes ).iterator().next();
        }
        return new TreeSet<>( basisTimes ).first();
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        if ( basisTimes.size() > 1 )
        {
            for ( TimeSeries<PairOfDoubles> next : basisTimeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        else
        {
            for ( Pair<Instant, PairOfDoubles> next : timeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        return joiner.toString();
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
         * A list of basis times associated with a baseline dataset. There are as many basis times as atomic time-series.
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

        /**
         * Adds an atomic time-series to the builder. If the basis time already exists, the values are appended 
         * (i.e. are assumed to represent later values). 
         * 
         * @param basisTime the basis time for the time-series
         * @param values the time-series values, ordered from earliest to latest
         * @return the builder
         */

        public SafeRegularTimeSeriesOfSingleValuedPairsBuilder addData( Instant basisTime,
                                                                        List<PairOfDoubles> values )
        {
            Objects.requireNonNull( basisTime, "Enter a non-null basis time for the time-series." );
            //Does basis time already exist?
            if ( basisTimes.contains( basisTime ) )
            {
                int index = basisTimes.indexOf( basisTime );
                int insertAt = timeStepCount.get( index ) * ( index + 1 );
                mainInput.addAll( insertAt, values );
                timeStepCount.set( index, timeStepCount.get( index ) + values.size() );
            }
            else
            {
                addData( values );
                timeStepCount.add( values.size() );
                basisTimes.add( basisTime );
            }
            return this;
        }

        /**
         * Adds an atomic time-series to the builder for a baseline. If the basis time already exists, the values are 
         * appended (i.e. are assumed to represent later values). 
         * 
         * @param basisTime the basis time for the time-series
         * @param values the time-series values, ordered from earliest to latest
         * @return the builder
         */

        public SafeRegularTimeSeriesOfSingleValuedPairsBuilder addDataForBaseline( Instant basisTime,
                                                                                   List<PairOfDoubles> values )
        {
            Objects.requireNonNull( basisTime, "Enter a non-null basis time for the baseline time-series." );
            //Does basis time already exist?
            if ( basisTimesBaseline.contains( basisTime ) )
            {
                int index = basisTimesBaseline.indexOf( basisTime );
                int insertAt = timeStepCountBaseline.get( index ) * ( index + 1 );
                baselineInput.addAll( insertAt, values );
                timeStepCountBaseline.set( index, timeStepCountBaseline.get( index ) + values.size() );
            }
            else
            {
                addDataForBaseline( values );
                timeStepCountBaseline.add( values.size() );
                basisTimesBaseline.add( basisTime );
            }
            return this;
        }

        /**
         * Adds a regular time-series to the builder.
         * 
         * @param timeSeries the regular time-series
         * @return the builder
         * @throws MetricInputException if the specified input is inconsistent with any existing input
         */

        public SafeRegularTimeSeriesOfSingleValuedPairsBuilder
                addTimeSeries( RegularTimeSeriesOfSingleValuedPairs timeSeries )
        {
            //Validate where possible
            if ( Objects.nonNull( timeStep ) && !timeSeries.getRegularDuration().equals( timeStep ) )
            {
                throw new MetricInputException( "The input time-series has a time-step of '"
                                                + timeSeries.getRegularDuration()
                                                + "', which is inconsistent with the time-step of the existing data, '"
                                                + timeStep
                                                + "'." );
            }
            setTimeStep( timeSeries.getRegularDuration() );
            //Add the main data
            for ( TimeSeries<PairOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                RegularTimeSeriesOfSingleValuedPairs next = (RegularTimeSeriesOfSingleValuedPairs) a;
                addData( next.getEarliestBasisTime(), next.getData() );
                setMetadata( next.getMetadata() );
                if ( next.hasBaseline() )
                {
                    addDataForBaseline( next.getEarliestBasisTime(), next.getDataForBaseline() );
                    setMetadataForBaseline( next.getMetadataForBaseline() );
                }
            }
            return this;
        }

        /**
         * Sets the time-step of the regular time-series.
         * 
         * @param timeStep the time-step of the regular time-series
         */

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
        this.timeStep = b.timeStep;
        //Set as unmodifiable maps
        this.basisTimes =
                Collections.unmodifiableList( b.basisTimes );
        this.basisTimesBaseline =
                Collections.unmodifiableList( b.basisTimesBaseline );
        this.timeStepCount = b.timeStepCount.get( 0 );
        //Validate additional parameters
        if ( Objects.isNull( this.timeStep ) )
        {
            throw new MetricInputException( "Specify a non-null timestep for the time-series." );
        }
        //Check the number of timesteps
        Set<Integer> times = new HashSet<>( b.timeStepCount );
        if ( times.size() > 1 )
        {
            throw new MetricInputException( "Cannot construct a regular time-series whose atomic time-series contain "
                                            + "a variable number of times: " + times.toString() + "." );
        }
        Set<Integer> baselineTimes = new HashSet<>( b.timeStepCountBaseline );
        if ( baselineTimes.size() > 1 )
        {
            throw new MetricInputException( "Cannot construct a regular time-series whose atomic baseline time-series "
                                            + "contain a variable number of times." );
        }
        if ( hasBaseline() && b.timeStepCountBaseline.get( 0 ) != this.timeStepCount )
        {
            throw new MetricInputException( "The number of times in the baseline does not match the number of times in "
                                            + "the main time-series ["
                                            + b.timeStepCountBaseline.get( 0 )
                                            + ", "
                                            + this.timeStepCount
                                            + "]." );
        }
        //Set the iterators
        basisTimeIterator = getBasisTimeIterator();
        leadTimeIterator = getLeadTimeIterator();
        timeIterator = getTimeIterator();
    }

    /**
     * Returns an {@link Iterable} view of the basis times.
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
                    private int returned = 0;
                    final Iterator<Instant> iterator = basisTimes.iterator();
                    final List<PairOfDoubles> data = getData();
                    final List<PairOfDoubles> baselineData = getDataForBaseline();

                    @Override
                    public boolean hasNext()
                    {
                        return returned < basisTimes.size();
                    }

                    @Override
                    public TimeSeries<PairOfDoubles> next()
                    {
                        if ( returned >= basisTimes.size() )
                        {
                            throw new NoSuchElementException( " No more basis times to iterate." );
                        }
                        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder =
                                new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
                        Instant nextTime = iterator.next();
                        int start = returned * timeStepCount;
                        builder.setTimeStep( timeStep );
                        builder.addData( nextTime, data.subList( start, start + timeStepCount ) );
                        builder.setMetadata( getMetadata() );
                        if ( hasBaseline() && basisTimesBaseline.contains( nextTime ) )
                        {
                            int startBase = basisTimesBaseline.indexOf( nextTime ) * timeStepCount;
                            builder.addDataForBaseline( nextTime,
                                                        baselineData.subList( startBase, startBase + timeStepCount ) );
                            builder.setMetadataForBaseline( getMetadataForBaseline() );
                        }
                        returned++;
                        return builder.build();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( UNSUPPORTED_MODIFICATION );
                    }
                };
            }
        }
        return new IterableTimeSeries();
    }

    /**
     * Returns an {@link Iterable} view of the lead times.
     * 
     * @return an iterable view of the lead times
     */

    private Iterable<TimeSeries<PairOfDoubles>> getLeadTimeIterator()
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<PairOfDoubles>>
        {
            @Override
            public Iterator<TimeSeries<PairOfDoubles>> iterator()
            {
                return new Iterator<TimeSeries<PairOfDoubles>>()
                {
                    private int returned = 0;
                    final List<PairOfDoubles> data = getData();
                    final List<PairOfDoubles> baselineData = getDataForBaseline();

                    @Override
                    public boolean hasNext()
                    {
                        return returned < timeStepCount;
                    }

                    @Override
                    public TimeSeries<PairOfDoubles> next()
                    {
                        if ( returned >= timeStepCount )
                        {
                            throw new NoSuchElementException( " No more lead times to iterate." );
                        }
                        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder =
                                new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
                        builder.setTimeStep( timeStep.multipliedBy( returned + 1 ) );
                        builder.setMetadata( getMetadata() );
                        int start = 0;
                        for ( Instant next : basisTimes )
                        {
                            List<PairOfDoubles> subset = new ArrayList<>();
                            subset.add( data.get( start + returned ) );
                            builder.addData( next, subset );
                            start += timeStepCount;
                        }
                        //Add filtered baseline data
                        if ( hasBaseline() )
                        {
                            start = 0; //Reset counter
                            for ( Instant next : basisTimesBaseline )
                            {
                                List<PairOfDoubles> subset = new ArrayList<>();
                                subset.add( baselineData.get( start + returned ) );
                                builder.addDataForBaseline( next, subset );
                                start += timeStepCount;
                            }
                            builder.setMetadataForBaseline( getMetadataForBaseline() );
                        }
                        returned++;
                        return builder.build();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( UNSUPPORTED_MODIFICATION );
                    }
                };
            }
        }
        return new IterableTimeSeries();
    }

    /**
     * Returns an {@link Iterable} view of the pairs of times and values.
     * 
     * @return an iterable view of the basis times
     */

    private Iterable<Pair<Instant, PairOfDoubles>> getTimeIterator()
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<Pair<Instant, PairOfDoubles>>
        {
            @Override
            public Iterator<Pair<Instant, PairOfDoubles>> iterator()
            {
                return new Iterator<Pair<Instant, PairOfDoubles>>()
                {
                    private int returned = 0;
                    final List<PairOfDoubles> data = getData();

                    @Override
                    public boolean hasNext()
                    {
                        return returned < data.size();
                    }

                    @Override
                    public Pair<Instant, PairOfDoubles> next()
                    {
                        if ( returned >= data.size() )
                        {
                            throw new NoSuchElementException( " No more pairs to iterate." );
                        }
                        Pair<Instant, PairOfDoubles> returnMe = new Pair<Instant, PairOfDoubles>()
                        {
                            int returnedSoFar = returned; //Current step, before incremented

                            @Override
                            public Instant getItemOne()
                            {
                                int basisIndex = (int) Math.floor( ( (double) returnedSoFar ) / timeStepCount );
                                int residual = returnedSoFar - ( basisIndex * timeStepCount );
                                return basisTimes.get( basisIndex ).plus( timeStep.multipliedBy( residual + 1 ) );
                            }

                            @Override
                            public PairOfDoubles getItemTwo()
                            {
                                return data.get( returnedSoFar );
                            }

                            @Override
                            public String toString()
                            {
                                return getItemOne() + "," + getItemTwo().getItemOne() + "," + getItemTwo().getItemTwo();
                            }

                        };
                        returned++;
                        return returnMe;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException( UNSUPPORTED_MODIFICATION );
                    }
                };
            }
        }
        return new IterableTimeSeries();
    }


}

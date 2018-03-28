package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.Predicate;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.builders.TimeSeriesOfSingleValuedPairsBuilder;
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

    private final SafeTimeSeriesOfPairs<PairOfDoubles> bP;

    @Override
    public TimeSeriesOfSingleValuedPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        SafeTimeSeriesOfSingleValuedPairsBuilder builder = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        builder.addTimeSeriesData( bP.getDataForBaseline() ).setMetadata( getMetadataForBaseline() );
        return builder.build();
    }

    @Override
    public Iterable<Event<PairOfDoubles>> timeIterator()
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
        SafeTimeSeriesOfSingleValuedPairsBuilder builder = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        for ( TimeSeries<PairOfDoubles> a : durationIterator() )
        {
            TimeSeriesOfSingleValuedPairs next = (TimeSeriesOfSingleValuedPairs) a;
            if ( duration.test( a.getDurations().first() ) )
            {
                builder.addTimeSeries( next );
            }
        }
        //Build if something to build
        if ( !builder.data.isEmpty() )
        {
            return builder.build();
        }
        return null;
    }

    @Override
    public TimeSeries<PairOfDoubles> filterByBasisTime( Predicate<Instant> basisTime )
    {
        Objects.requireNonNull( basisTime, "Provide a non-null predicate on which to filter by basis time." );
        SafeTimeSeriesOfSingleValuedPairsBuilder builder = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        //Add the filtered data
        for ( TimeSeries<PairOfDoubles> a : basisTimeIterator() )
        {
            if ( basisTime.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( (TimeSeriesOfSingleValuedPairs) a );
            }
        }
        //Build if something to build
        if ( !builder.data.isEmpty() )
        {
            return builder.build();
        }
        return null;
    }

    @Override
    public List<Instant> getBasisTimes()
    {
        return Collections.unmodifiableList( bP.getBasisTimes() );
    }

    @Override
    public SortedSet<Duration> getDurations()
    {
        return Collections.unmodifiableSortedSet( bP.getDurations() );
    }

    @Override
    public boolean hasMultipleTimeSeries()
    {
        return bP.hasMultipleTimeSeries();
    }

    @Override
    public boolean isRegular()
    {
        return bP.isRegular();
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

        private List<Event<List<Event<PairOfDoubles>>>> baselineData = new ArrayList<>();

        @Override
        public SafeTimeSeriesOfSingleValuedPairsBuilder
                addTimeSeriesData( List<Event<List<Event<PairOfDoubles>>>> values )
        {
            List<Event<List<Event<PairOfDoubles>>>> sorted = TimeSeriesHelper.sort( values );
            data.addAll( sorted );
            addData( TimeSeriesHelper.unwrap( sorted ) );
            return this;
        }

        @Override
        public SafeTimeSeriesOfSingleValuedPairsBuilder
                addTimeSeriesDataForBaseline( List<Event<List<Event<PairOfDoubles>>>> values )
        {
            List<Event<List<Event<PairOfDoubles>>>> sorted = TimeSeriesHelper.sort( values );
            baselineData.addAll( sorted );
            addDataForBaseline( TimeSeriesHelper.unwrap( sorted ) );
            return this;
        }

        @Override
        public SafeTimeSeriesOfSingleValuedPairsBuilder
                addTimeSeries( TimeSeriesOfSingleValuedPairs timeSeries )
        {
            List<Metadata> mainMeta = new ArrayList<>();
            VectorOfDoubles climatology = null;
            MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
            
            for ( TimeSeries<PairOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                //Add the main data
                TimeSeriesOfSingleValuedPairs next = (TimeSeriesOfSingleValuedPairs) a;

                List<Event<PairOfDoubles>> nextSource = new ArrayList<>();
                for ( Event<PairOfDoubles> nextEvent : next.timeIterator() )
                {
                    nextSource.add( nextEvent );
                }
                addTimeSeriesData( Arrays.asList( Event.of( next.getEarliestBasisTime(), nextSource ) ) );
                mainMeta.add( next.getMetadata() );
                
                //Add climatology if available
                if ( next.hasClimatology() )
                {
                    climatology = next.getClimatology();
                }
            }
            setMetadata( metaFac.unionOf( mainMeta ) );

            //Add the baseline data if required
            if ( timeSeries.hasBaseline() )
            {
                this.addTimeSeriesForBaseline( timeSeries.getBaselineData() );
            }

            setClimatology( climatology );
            return this;
        }

        @Override
        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesForBaseline( TimeSeriesOfSingleValuedPairs timeSeries )
        {
            List<Metadata> baselineMeta = new ArrayList<>();
            MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
            for ( TimeSeries<PairOfDoubles> a : timeSeries.basisTimeIterator() )
            {
                //Add the main data
                TimeSeriesOfSingleValuedPairs next = (TimeSeriesOfSingleValuedPairs) a;

                List<Event<PairOfDoubles>> nextSource = new ArrayList<>();

                for ( Event<PairOfDoubles> nextEvent : next.timeIterator() )
                {
                    nextSource.add( nextEvent );
                }
                addTimeSeriesDataForBaseline( Arrays.asList( Event.of( next.getEarliestBasisTime(), nextSource ) ) );

                baselineMeta.add( next.getMetadata() );
            }

            setMetadataForBaseline( metaFac.unionOf( baselineMeta ) );

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
        bP = new SafeTimeSeriesOfPairs<>( b.data,
                                          b.baselineData,
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

                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    @Override
                    public TimeSeries<PairOfDoubles> next()
                    {
                        if ( !hasNext() )
                        {
                            throw new NoSuchElementException( "No more basis times to iterate." );
                        }
                        SafeTimeSeriesOfSingleValuedPairsBuilder builder =
                                new SafeTimeSeriesOfSingleValuedPairsBuilder();
                        
                        // Iterate
                        iterator.next();
                        
                        builder.addTimeSeriesData( Arrays.asList( bP.getData().get( returned ) ) );

                        // Propagate the metadata without adjustment because the input period is canonical
                        builder.setMetadata( getMetadata() );
                        
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
                    Iterator<Duration> iterator = bP.getDurations().iterator();

                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    @Override
                    public TimeSeries<PairOfDoubles> next()
                    {
                        if ( !hasNext() )
                        {
                            throw new NoSuchElementException( "No more durations to iterate." );
                        }
                        SafeTimeSeriesOfSingleValuedPairsBuilder builder =
                                new SafeTimeSeriesOfSingleValuedPairsBuilder();
                        
                        // Iterate
                        Duration nextDuration = iterator.next();

                        //Adjust the time window for the metadata
                        builder.setMetadata( TimeSeriesHelper.getDurationAdjustedMetadata( getMetadata(),
                                                                                           nextDuration,
                                                                                           nextDuration ) );
                        // Data for the current duration by basis time
                        builder.addTimeSeriesData( bP.filterByDuration( nextDuration, bP.getData() ) );
                        // Set the climatology
                        builder.setClimatology( getClimatology() );
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

package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleDataException;

/**
 * Immutable base class for a time-series.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 */
public class BasicTimeSeries<T> implements TimeSeries<T>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( BasicTimeSeries.class );

    /**
     * The raw data, which retains the composition of each individual time-series.
     */

    private final List<List<Event<T>>> data;

    /**
     * Basis times for the data.
     */

    private final SortedSet<Instant> basisTimes;

    /**
     * Durations associated with the time-series.
     */

    private final SortedSet<Duration> durations;

    /**
     * Basis time iterator.
     */

    private final Iterable<TimeSeries<T>> basisTimeIterator;

    /**
     * Duration iterator.
     */

    private final Iterable<TimeSeries<T>> durationIterator;

    /**
     * Event iterator.
    */

    private final Iterable<Event<T>> timeIterator;

    /**
     * Builds a time-series from the input. The outer list composes the individual time-series and the inner list 
     * composes the events associated with each time-series.
     * 
     * @param <T> the type of event
     * @param timeSeries the input time-series data
     * @return a time-series
     */

    public static <T> BasicTimeSeries<T> of( List<List<Event<T>>> timeSeries )
    {
        return new BasicTimeSeries<>( timeSeries );
    }

    /**
     * A default builder to build a time-series incrementally. Also see {@link BasicTimeSeries#of(List)}.
     */

    public static class BasicTimeSeriesBuilder<T> implements TimeSeriesBuilder<T>
    {

        /**
         * The raw data.
         */

        private List<List<Event<T>>> data = new ArrayList<>();

        @Override
        public BasicTimeSeries<T> build()
        {
            return new BasicTimeSeries<>( this );
        }

        @Override
        public TimeSeriesBuilder<T> addTimeSeries( List<Event<T>> timeSeries )
        {
            this.data.add( timeSeries );

            return this;
        }

    }

    @Override
    public Iterable<Event<T>> timeIterator()
    {
        return timeIterator;
    }

    @Override
    public Iterable<TimeSeries<T>> basisTimeIterator()
    {
        return basisTimeIterator;
    }

    @Override
    public Iterable<TimeSeries<T>> durationIterator()
    {
        return durationIterator;
    }

    @Override
    public SortedSet<Instant> getBasisTimes()
    {
        return Collections.unmodifiableSortedSet( this.basisTimes );
    }

    @Override
    public Instant getEarliestBasisTime()
    {
        if ( this.getBasisTimes().isEmpty() )
        {
            return Instant.MIN;
        }

        return this.getBasisTimes().first();
    }

    @Override
    public Instant getLatestBasisTime()
    {
        if ( this.getBasisTimes().isEmpty() )
        {
            return Instant.MAX;
        }
        return this.getBasisTimes().last();
    }

    @Override
    public SortedSet<Duration> getDurations()
    {
        return Collections.unmodifiableSortedSet( this.durations );
    }

    @Override
    public boolean hasMultipleTimeSeries()
    {
        return data.size() > 1;
    }

    @Override
    public boolean isRegular()
    {
        return Objects.nonNull( this.getRegularDuration() );
    }

    @Override
    public Duration getRegularDuration()
    {
        if ( this.durations.size() == 1 )
        {
            return this.durations.first();
        }
        Duration gap = null;
        Duration last = null;
        for ( Duration next : this.durations )
        {
            if ( Objects.isNull( last ) )
            {
                last = this.durations.first();
            }
            else if ( Objects.isNull( gap ) )
            {
                gap = next.minus( last );
                last = next;
            }
            else if ( !next.minus( last ).equals( gap ) )
            {
                return null;
            }
            else
            {
                last = next;
            }
        }
        return gap;
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this );
    }

    /**
     * Build the time-series.
     * 
     * @param data the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param basisTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws SampleDataException if one or more inputs is invalid
     */

    BasicTimeSeries( final BasicTimeSeriesBuilder<T> builder )
    {
        this( builder.data );
    }

    /**
     * Build the time-series internally.
     * 
     * @param data the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param basisTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws SampleDataException if one or more inputs is invalid
     */

    BasicTimeSeries( final List<List<Event<T>>> data )
    {
        // Time-series cannot be null
        if ( Objects.isNull( data ) )
        {
            throw new SampleDataException( "The time-series input cannot be null." );
        }

        // Sets and validates       
        List<List<Event<T>>> localData = new ArrayList<>();

        for ( List<Event<T>> nextList : data )
        {
            // Check for null values
            if ( Objects.isNull( nextList ) )
            {
                throw new SampleDataException( "One or more time-series is null." );
            }

            List<Event<T>> localList = new ArrayList<>();

            localList.addAll( nextList );

            if ( localList.contains( null ) )
            {
                throw new SampleDataException( "One or more time-series has null values." );
            }

            // Sort in time order
            Collections.sort( localList );

            localData.add( Collections.unmodifiableList( localList ) );
        }

        this.data = Collections.unmodifiableList( localData );

        // Set the durations
        this.durations = this.data.stream()
                                  .flatMap( List::stream )
                                  .map( Event::getDuration )
                                  .collect( Collectors.toCollection( TreeSet::new ) );

        // Set the basis times
        this.basisTimes = this.data.stream()
                                   .flatMap( List::stream )
                                   .map( Event::getReferenceTime )
                                   .collect( Collectors.toCollection( TreeSet::new ) );

        // Set the iterators
        this.basisTimeIterator = BasicTimeSeries.getBasisTimeIterator( this.data );

        this.durationIterator = BasicTimeSeries.getDurationIterator( this.data, this.durations );

        this.timeIterator = BasicTimeSeries.getTimeIterator( this.data );
    }

    /**
     * Returns an {@link Iterable} view of the {@link Event}.
     * 
     * @param data the data
     * @return an iterable view of the times and values
     */

    private static <T> Iterable<Event<T>> getTimeIterator( final List<List<Event<T>>> data )
    {
        // Unpack
        List<Event<T>> unpacked = data.stream().flatMap( List::stream ).collect( Collectors.toList() );

        //Construct an iterable view of the times and values
        class IterableTimeSeries implements Iterable<Event<T>>
        {
            @Override
            public Iterator<Event<T>> iterator()
            {
                return new Iterator<Event<T>>()
                {
                    int returned = 0;

                    @Override
                    public boolean hasNext()
                    {
                        return returned < unpacked.size();
                    }

                    @Override
                    public Event<T> next()
                    {
                        if ( returned >= unpacked.size() )
                        {
                            throw new NoSuchElementException( "No more events to iterate." );
                        }

                        Event<T> returnMe = unpacked.get( returned );

                        returned++;

                        return returnMe;
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
     * Returns an {@link Iterable} view of the atomic time-series by basis time.
     * 
     * @param data the input data to iterate
     * @return an iterable view of the basis times
     */

    private static <T> Iterable<TimeSeries<T>> getBasisTimeIterator( final List<List<Event<T>>> data )
    {
        // The atomic time-series, one for each basis time in each inner list
        final List<List<Event<T>>> atomic = new ArrayList<>();

        LOGGER.trace( "Creating an iterator for the reference times." );

        // Create a separate time series per reference time
        // within each inner list of time-series
        // see #58655
        for ( List<Event<T>> nextSeries : data )
        {
            // Group events by reference time
            // Each group represents one time-series
            // Place in a sorted map from earliest to latest times
            SortedMap<Instant, List<Event<T>>> grouped =
                    nextSeries.stream()
                              .collect( Collectors.groupingBy( Event::getReferenceTime,
                                                               TreeMap::new,
                                                               Collectors.toList() ) );

            // Collect into the list of lists, which will preserve 
            // multiple time-series with the same reference times
            grouped.forEach( ( key, value ) -> atomic.add( value ) );
        }

        LOGGER.trace( "Finished creating an iterator for the reference times." );

        // Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<T>>
        {

            @Override
            public Iterator<TimeSeries<T>> iterator()
            {
                return new Iterator<TimeSeries<T>>()
                {
                    int returned = 0;

                    @Override
                    public boolean hasNext()
                    {
                        return returned < atomic.size();
                    }

                    @Override
                    public TimeSeries<T> next()
                    {
                        if ( !hasNext() )
                        {
                            throw new NoSuchElementException( "No more basis times to iterate." );
                        }

                        BasicTimeSeries<T> returnMe = new BasicTimeSeries<>( Arrays.asList( atomic.get( returned ) ) );

                        returned++;

                        return returnMe;
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
     * @param data the input data to iterate
     * @return an iterable view of the durations
     */

    private static <T> Iterable<TimeSeries<T>> getDurationIterator( final List<List<Event<T>>> data,
                                                                    final SortedSet<Duration> durations )
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<T>>
        {
            @Override
            public Iterator<TimeSeries<T>> iterator()
            {
                return new Iterator<TimeSeries<T>>()
                {
                    Iterator<Duration> iterator = durations.iterator();

                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    @Override
                    public TimeSeries<T> next()
                    {
                        if ( !hasNext() )
                        {
                            throw new NoSuchElementException( "No more durations to iterate." );
                        }

                        // Iterate
                        Duration nextDuration = iterator.next();

                        return Slicer.filterByDuration( new BasicTimeSeries<>( data ),
                                                        isEqual -> isEqual.equals( nextDuration ) );
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

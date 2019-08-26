package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.datamodel.sampledata.SampleDataException;

/**
 * Immutable base class for a time-series.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 */
public class BasicTimeSeries<T> implements TimeSeriesCollection<T>
{

    /**
     * The raw data, which retains the composition of each individual time-series.
     */

    private final List<TimeSeries<T>> timeSeries;

    /**
     * Basis times for the data.
     */

    private final SortedSet<Instant> basisTimes;

    /**
     * Durations associated with the time-series.
     */

    private final SortedSet<Duration> durations;

    /**
     * Event iterator.
    */

    private final Iterable<Event<T>> eventIterator;

    /**
     * Builds a time-series from the input.
     * 
     * @param <T> the type of event
     * @param timeSeries the input time-series data
     * @return a time-series
     */

    public static <T> BasicTimeSeries<T> of( List<TimeSeries<T>> timeSeries )
    {
        return new BasicTimeSeries<>( timeSeries );
    }

    /**
     * A default builder to build a time-series incrementally. Also see {@link BasicTimeSeries#of(List)}.
     */

    public static class BasicTimeSeriesBuilder<T> implements TimeSeriesCollectionBuilder<T>
    {

        /**
         * The raw data.
         */

        private List<TimeSeries<T>> data = new ArrayList<>();

        @Override
        public BasicTimeSeries<T> build()
        {
            return new BasicTimeSeries<>( this );
        }

        @Override
        public TimeSeriesCollectionBuilder<T> addTimeSeries( TimeSeries<T> timeSeries )
        {
            this.data.add( timeSeries );

            return this;
        }

    }

    @Override
    public Iterable<Event<T>> eventIterator()
    {
        return eventIterator;
    }
    
    @Override
    public List<TimeSeries<T>> getTimeSeries()
    {
        return Collections.unmodifiableList( this.timeSeries );
    }

    @Override
    public SortedSet<Instant> getReferenceTimes()
    {
        return Collections.unmodifiableSortedSet( this.basisTimes );
    }

    @Override
    public SortedSet<Duration> getDurations()
    {
        return Collections.unmodifiableSortedSet( this.durations );
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this );
    }

    /**
     * Build the time-series.
     * 
     * @param timeSeries the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param referenceTimeIterator a basis time iterator
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
     * @param referenceTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws SampleDataException if one or more inputs is invalid
     */

    BasicTimeSeries( final List<TimeSeries<T>> data )
    {
        // Time-series cannot be null
        if ( Objects.isNull( data ) )
        {
            throw new SampleDataException( "The time-series input cannot be null." );
        }

        // Sets and validates       
        if ( data.contains( null ) )
        {
            throw new SampleDataException( "One or more time-series is null." );
        }

        this.timeSeries = Collections.unmodifiableList( data );
        
        // Set the durations
        this.durations = this.timeSeries.stream()
                                  .map( TimeSeries::getEvents )
                                  .flatMap( SortedSet::stream )
                                  .map( Event::getDuration )
                                  .collect( Collectors.toCollection( TreeSet::new ) );

        // Set the basis times
        this.basisTimes = this.timeSeries.stream()
                                   .map( TimeSeries::getEvents )
                                   .flatMap( SortedSet::stream )
                                   .map( Event::getReferenceTime )
                                   .collect( Collectors.toCollection( TreeSet::new ) );

        this.eventIterator = BasicTimeSeries.getEventIterator( this.timeSeries );
    }

    /**
     * Returns an {@link Iterable} view of the {@link Event}.
     * 
     * @param data the data
     * @return an iterable view of the times and values
     */

    private static <T> Iterable<Event<T>> getEventIterator( final List<TimeSeries<T>> data )
    {
        // Unpack
        List<Event<T>> unpacked =
                data.stream().map( TimeSeries::getEvents ).flatMap( SortedSet::stream ).collect( Collectors.toList() );

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

}

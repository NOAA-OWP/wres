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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Base class for an immutable implementation of a (possibly irregular) time-series.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 */
class SafeTimeSeries<T> implements TimeSeries<T>
{

    /**
     * The raw data.
     */

    private final List<Event<List<Event<T>>>> data;

    /**
     * The raw data for the baseline
     */

    private final List<Event<List<Event<T>>>> baselineData;

    /**
     * Basis times for the data.
     */

    private final List<Instant> basisTimes;

    /**
     * Durations associated with the time-series.
     */

    private final SortedSet<Duration> durations;

    /**
     * Durations associated with the baseline time-series.
     */

    private final SortedSet<Duration> durationsBaseline;

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
    public List<Instant> getBasisTimes()
    {
        return Collections.unmodifiableList( this.basisTimes );
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
     * Returns the immutable raw data.
     * 
     * @return the raw data
     */

    List<Event<List<Event<T>>>> getRawData()
    {
        // Rendered immutable on construction
        return this.data;
    }

    /**
     * Returns the immutable raw data for the baseline.
     * 
     * @return the raw data for the baseline
     */

    List<Event<List<Event<T>>>> getRawDataForBaseline()
    {
        // Rendered immutable on construction
        return this.baselineData;
    }

    /**
     * Build the time-series.
     * 
     * @param data the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param basisTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws MetricInputException if one or more inputs is invalid
     */

    SafeTimeSeries( final List<Event<List<Event<T>>>> data,
                    final List<Event<List<Event<T>>>> baselineData )
    {
        this( data,
              baselineData,
              SafeTimeSeries.getBasisTimeIterator( data ),
              SafeTimeSeries.getDurationIterator( data ) );
    }

    /**
     * Build the time-series.
     * 
     * @param data the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param basisTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws MetricInputException if one or more inputs is invalid
     */

    SafeTimeSeries( final List<Event<List<Event<T>>>> data,
                    final List<Event<List<Event<T>>>> baselineData,
                    final Iterable<TimeSeries<T>> basisTimeIterator,
                    final Iterable<TimeSeries<T>> durationIterator )
    {

        // Set then validate
        this.data = TimeSeriesHelper.getImmutableTimeSeries( data );

        // Baseline data?
        if ( Objects.nonNull( baselineData ) )
        {
            this.baselineData = TimeSeriesHelper.getImmutableTimeSeries( baselineData );
        }
        else
        {
            this.baselineData = null;
        }

        // Set the iterators
        this.basisTimeIterator = basisTimeIterator;
        this.durationIterator = durationIterator;

        // Set the durations
        this.durations = new TreeSet<>();
        int eventCount = 0;
        for ( Event<List<Event<T>>> nextSeries : this.data )
        {
            Instant basisTime = nextSeries.getTime();
            for ( Event<T> nextEvent : nextSeries.getValue() )
            {
                eventCount++;
                this.durations.add( Duration.between( basisTime, nextEvent.getTime() ) );
            }
        }
        // Set the basis times
        this.basisTimes = this.data.stream().map( Event::getTime ).collect( Collectors.toList() );
        if ( Objects.nonNull( baselineData ) )
        {
            this.durationsBaseline = new TreeSet<>();
            for ( Event<List<Event<T>>> nextSeries : this.baselineData )
            {
                Instant basisTime = nextSeries.getTime();
                for ( Event<T> nextEvent : nextSeries.getValue() )
                {
                    this.durationsBaseline.add( Duration.between( basisTime, nextEvent.getTime() ) );
                }
            }
        }
        else
        {
            this.durationsBaseline = null;
        }
        // Set the time iterator
        timeIterator = SafeTimeSeries.getTimeIterator( this.data, eventCount );
    }

    /**
     * Returns an {@link Iterable} view of the {@link Event}.
     * 
     * @param data the data
     * @param eventCount the total number of events across all time-series
     * @return an iterable view of the times and values
     */

    private static <T> Iterable<Event<T>> getTimeIterator( final List<Event<List<Event<T>>>> data,
                                                           final int eventCount )
    {
        //Construct an iterable view of the times and values
        class IterableTimeSeries implements Iterable<Event<T>>
        {
            @Override
            public Iterator<Event<T>> iterator()
            {
                return new Iterator<Event<T>>()
                {
                    int returned = 0;
                    int timeSeriesIndex = 0;
                    int timeIndex = 0;

                    @Override
                    public boolean hasNext()
                    {
                        return returned < eventCount;
                    }

                    @Override
                    public Event<T> next()
                    {
                        if ( returned >= eventCount )
                        {
                            throw new NoSuchElementException( "No more events to iterate." );
                        }
                        Event<List<Event<T>>> nextList = data.get( timeSeriesIndex );
                        Event<T> returnMe = nextList.getValue().get( timeIndex );
                        returned++;
                        timeIndex++;
                        // Roll over to next atomic time-series
                        if ( timeIndex == data.get( timeSeriesIndex ).getValue().size() )
                        {
                            timeSeriesIndex++;
                            timeIndex = 0;
                        }
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

    private static <T> Iterable<TimeSeries<T>> getBasisTimeIterator( final List<Event<List<Event<T>>>> data )
    {
        //Construct an iterable view of the basis times
        class IterableTimeSeries implements Iterable<TimeSeries<T>>
        {
            // Basis times
            List<Instant> basisTimes = data.stream().map( Event::getTime ).collect( Collectors.toList() );

            @Override
            public Iterator<TimeSeries<T>> iterator()
            {
                return new Iterator<TimeSeries<T>>()
                {
                    int returned = 0;
                    Iterator<Instant> iterator = basisTimes.iterator();

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
                            throw new NoSuchElementException( "No more basis times to iterate." );
                        }

                        // Iterate
                        iterator.next();

                        List<Event<List<Event<T>>>> events = Arrays.asList( data.get( returned ) );

                        returned++;
                        return new SafeTimeSeries<>( events, null );
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

    private static <T> Iterable<TimeSeries<T>> getDurationIterator( final List<Event<List<Event<T>>>> data )
    {
        // Determine the durations
        Set<Duration> durations = new TreeSet<>();
        for ( Event<List<Event<T>>> nextSeries : data )
        {
            Instant basisTime = nextSeries.getTime();
            for ( Event<T> nextEvent : nextSeries.getValue() )
            {
                durations.add( Duration.between( basisTime, nextEvent.getTime() ) );
            }
        }

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

                        // Data for the current duration by basis time
                        List<Event<List<Event<T>>>> events = SafeTimeSeries.filterByDuration( nextDuration, data );

                        return new SafeTimeSeries<>( events, null );
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
     * Filters the data by {@link Duration}.
     * 
     * TODO: remove this method once a general builder of time-series is public facing and the {@link Slicer} provides
     * a filtering method for generic time-series.
     *
     * @param duration the duration to find
     * @param rawData the raw data to search 
     * @return the filtered data
     */

    static <T> List<Event<List<Event<T>>>> filterByDuration( final Duration duration,
                                                             final List<Event<List<Event<T>>>> rawData )
    {
        List<Event<List<Event<T>>>> returnMe = new ArrayList<>();
        // Iterate through basis times
        for ( Event<List<Event<T>>> nextSeries : rawData )
        {
            Instant basisTime = nextSeries.getTime();
            // Iterate through durations until it is later than the nextDuration
            for ( Event<T> nextEvent : nextSeries.getValue() )
            {
                Duration candidateDuration = Duration.between( basisTime, nextEvent.getTime() );
                if ( candidateDuration.compareTo( duration ) > 0 )
                {
                    break;
                }
                else if ( duration.equals( candidateDuration ) )
                {
                    returnMe.add( Event.of( basisTime, Arrays.asList( nextEvent ) ) );
                }
            }
        }
        return returnMe;
    }

}

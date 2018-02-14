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
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Base class for an immutable implementation of a (possibly irregular) time-series of pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
class SafeTimeSeriesOfPairs<T>
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

    /**
     * Returns the immutable raw data.
     * 
     * @return the raw data
     */

    List<Event<List<Event<T>>>> getData()
    {
        // Return immutable data
        return data;
    }
    
    /**
     * Returns the immutable raw data for the baseline.
     * 
     * @return the raw data for the baseline
     */

    List<Event<List<Event<T>>>> getDataForBaseline()
    {
        // Return immutable data
        return baselineData;
    }    
    
    /**
     * Returns an iterator over each pair.
     * 
     * @return an iterator over each pair
     */

    Iterable<Event<T>> timeIterator()
    {
        return timeIterator;
    }

    /**
     * Returns an iterator over the basis times.
     * 
     * @return an iterator over the basis times
     */

    Iterable<TimeSeries<T>> basisTimeIterator()
    {
        return basisTimeIterator;
    }

    /**
     * Returns an iterator over the durations.
     * 
     * @return an iterator over the durations
     */

    Iterable<TimeSeries<T>> durationIterator()
    {
        return durationIterator;
    }

    /**
     * Returns the basis times.
     * 
     * @return the basis times
     */
    List<Instant> getBasisTimes()
    {
        return Collections.unmodifiableList( basisTimes );
    }

    /**
     * Returns the durations.
     * 
     * @return the durations
     */
    
    SortedSet<Duration> getDurations()
    {
        return Collections.unmodifiableSortedSet( durations );
    }

    /**
     * Returns <code>true</code> if the store contains multiple atomic time-series, otherwise <code>false</code>.
     * 
     * @return true if there are multiple atomic time-series, false otherwise
     */
    
    boolean hasMultipleTimeSeries()
    {
        return data.size() > 1;
    }

    /**
     * Returns <code>true</code> if the time-series is regular, otherwise <code>false</code>.
     * 
     * @return true if the time-series is regular, false otherwise
     */
    
    boolean isRegular()
    {
        return Objects.nonNull( getRegularDuration() );
    }

    /**
     * Returns the regular duration associated with the time-series or null if the time-series is irregular.
     * 
     * @return the regular duration or null
     */
    
    Duration getRegularDuration()
    {
        if ( durations.size() == 1 )
        {
            return durations.first();
        }
        Duration gap = null;
        Duration last = null;
        for ( Duration next : durations )
        {
            if ( Objects.isNull( last ) )
            {
                last = durations.first();
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
            else {
                last = next;
            }
        }
        return gap;
    }

    /**
     * Build the time-series of pairs.
     * 
     * @param data the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param basisTimes the basis times
     * @param durations the durations
     * @param durationsBaseline the durations for the baseline
     * @param basisTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws MetricInputException if one or more inputs is invalid
     */
    
    SafeTimeSeriesOfPairs( final List<Event<List<Event<T>>>> data,
                           final List<Event<List<Event<T>>>> baselineData,
                           final Iterable<TimeSeries<T>> basisTimeIterator,
                           final Iterable<TimeSeries<T>> durationIterator )
    {

        // Set then validate
        this.data = TimeSeriesHelper.getImmutableTimeSeries( data );
        this.baselineData = TimeSeriesHelper.getImmutableTimeSeries( baselineData );
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
        if (! this.baselineData.isEmpty() )
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
        timeIterator = getTimeIterator( eventCount );
    }

    /**
     * Returns an {@link Iterable} view of the pairs of times and values.
     * 
     * @param eventCount the total number of events across all time-series
     * @return an iterable view of the times and values
     */

    private Iterable<Event<T>> getTimeIterator( int eventCount )
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
                            throw new NoSuchElementException( "No more pairs to iterate." );
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
     * Filters the data by {@link Duration}.
     *
     * @param duration the duration to find
     * @param the raw data to search 
     * @return the filtered data
     */

    List<Event<List<Event<T>>>> filterByDuration( Duration duration,
                                                                      List<Event<List<Event<T>>>> rawData )
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

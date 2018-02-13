package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
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

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Base class for an immutable implementation of a time-series of pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
class SafeRegularTimeSeriesOfPairs<T>
{

    /**
     * The time-step associated with the regular time-series.
     */

    private final Duration timeStep;

    /**
     * A list of basis times. There are as many basis times as atomic time-series.
     */

    private final List<Instant> basisTimes;

    /**
     * A list of basis times associated with a baseline dataset. There are as many basis times as atomic time-series.
     */

    private final List<Instant> basisTimesBaseline;

    /**
     * The number of times in each atomic time-series.
     */

    private final int timeStepCount;

    /**
     * Iterable view of the basis times.
     */

    private final Iterable<TimeSeries<T>> basisTimeIterator;

    /**
     * Iterable view of the durations.
     */

    private final Iterable<TimeSeries<T>> durationIterator;

    /**
     * Iterable view of the pairs of times and values. 
     */

    private final Iterable<Event<T>> timeIterator;

    /**
     * Error message denoting attempt to modify an immutable time-series via an iterator.
     */

    static final String UNSUPPORTED_MODIFICATION = "While attempting to modify an immutable time-series.";

    /**
     * Helper method that adjusts the earliest and latest basis times of the {@link TimeWindow} associated with the 
     * input {@link Metadata} when iterating over the atomic time-series by basis time.
     * 
     * @param input the input metadata
     * @param earliestTime the earliest basis time for the new metadata
     * @param latestTime the latest basis time for the new metadata
     * @return the adjusted metadata
     * @throws NullPointerException if any of the inputs are null
     */

    static Metadata getBasisTimeAdjustedMetadata( Metadata input, Instant earliestTime, Instant latestTime )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        Metadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = DefaultMetadataFactory.getInstance()
                                             .getMetadata( returnMe,
                                                           TimeWindow.of( earliestTime,
                                                                          latestTime,
                                                                          current.getReferenceTime(),
                                                                          current.getEarliestLeadTime(),
                                                                          current.getLatestLeadTime() ) );
        }
        return returnMe;
    }

    /**
     * Helper method that adjusts the earliest and latest durations of the {@link TimeWindow} associated with the input
     * {@link Metadata} when iterating over the atomic time-series by duration.
     * 
     * @param input the input metadata
     * @param earliestDuration the earliest duration for the new metadata
     * @param latestDuration the latest duration for the new metadata
     * @return the adjusted metadata
     * @throws NullPointerException if any of the inputs are null
     */

    static Metadata getDurationAdjustedMetadata( Metadata input, Duration earliestDuration, Duration latestDuration )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        Metadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = DefaultMetadataFactory.getInstance()
                                             .getMetadata( returnMe,
                                                           TimeWindow.of( current.getEarliestTime(),
                                                                          current.getLatestTime(),
                                                                          current.getReferenceTime(),
                                                                          earliestDuration,
                                                                          latestDuration ) );
        }
        return returnMe;
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
     * Returns the basis times for the baseline data.
     * 
     * @return the basis times for the baseline data
     */

    List<Instant> getBasisTimesBaseline()
    {
        return Collections.unmodifiableList( basisTimesBaseline );
    }

    /**
     * Returns the durations.
     * 
     * @return the durations
     */

    SortedSet<Duration> getDurations()
    {
        SortedSet<Duration> returnMe = new TreeSet<>();
        for ( long i = 0; i < timeStepCount; i++ )
        {
            returnMe.add( timeStep.multipliedBy( i + 1 ) );
        }
        return returnMe;
    }

    /**
     * Returns true if the store contains multiple time-series, false otherwise.
     * 
     * @return true if the store contains multiple time-series, false otherwise
     */

    boolean hasMultipleTimeSeries()
    {
        return basisTimes.size() > 1;
    }

    /**
     * Returns the regular duration.
     * 
     * @return the regular duration
     */

    Duration getRegularDuration()
    {
        return timeStep;
    }

    /**
     * Returns the number of timesteps in each time-series.
     * 
     * @return the number of timesteps
     */

    int getTimeStepCount()
    {
        return timeStepCount;
    }

    /**
     * Returns the earliest basis time.
     * 
     * @return the earliest basis time
     */

    Instant getEarliestBasisTime()
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
            for ( TimeSeries<T> next : basisTimeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        else
        {
            for ( Event<T> next : timeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        return joiner.toString();
    }

    /**
     * Construct the pairs.
     * 
     * @param data the data
     * @param timeStep the timestep 
     * @param basisTimes the basis times
     * @param basisTimesBaseline the basis times for the baseline
     * @param timeStepCount the number of timesteps per time-series
     * @param timeStepCountBaseline the number of timesteps in the baseline per time-series
     * @param basisTimeIterator the basis time iterator
     * @param durationIterator the duration iterator
     * @throws MetricInputException if the pairs are invalid
     */

    SafeRegularTimeSeriesOfPairs( final List<T> data,
                                  final Duration timeStep,
                                  final List<Instant> basisTimes,
                                  final List<Instant> basisTimesBaseline,
                                  List<Integer> timeStepCount,
                                  List<Integer> timeStepCountBaseline,
                                  Iterable<TimeSeries<T>> basisTimeIterator,
                                  Iterable<TimeSeries<T>> durationIterator )
    {
        this.timeStep = timeStep;
        //Set as unmodifiable lists
        this.basisTimes =
                Collections.unmodifiableList( basisTimes );
        this.basisTimesBaseline =
                Collections.unmodifiableList( basisTimesBaseline );
        this.timeStepCount = timeStepCount.get( 0 );
        //Validate additional parameters
        if ( Objects.isNull( this.timeStep ) )
        {
            throw new MetricInputException( "Specify a non-null timestep for the time-series." );
        }
        //Check the number of timesteps
        Set<Integer> times = new HashSet<>( timeStepCount );
        if ( times.size() > 1 )
        {
            throw new MetricInputException( "Cannot construct a regular time-series whose atomic time-series contain "
                                            + "a variable number of times: " + times.toString() + "." );
        }
        Set<Integer> baselineTimes = new HashSet<>( timeStepCountBaseline );
        if ( baselineTimes.size() > 1 )
        {
            throw new MetricInputException( "Cannot construct a regular time-series whose atomic baseline time-series "
                                            + "contain a variable number of times." );
        }
        if ( !timeStepCountBaseline.isEmpty() && timeStepCountBaseline.get( 0 ) != this.timeStepCount )
        {
            throw new MetricInputException( "The number of times in the baseline does not match the number of times in "
                                            + "the main time-series ["
                                            + timeStepCountBaseline.get( 0 )
                                            + ", "
                                            + this.timeStepCount
                                            + "]." );
        }
        this.basisTimeIterator = basisTimeIterator;
        this.durationIterator = durationIterator;
        this.timeIterator = getTimeIterator( data );
    }

    /**
     * Returns an {@link Iterable} view of the pairs of times and values.
     * 
     * @param data the data to iterate
     * @return an iterable view of the times and values
     */

    private Iterable<Event<T>> getTimeIterator( final List<T> data )
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

                    @Override
                    public boolean hasNext()
                    {
                        return returned < data.size();
                    }

                    @Override
                    public Event<T> next()
                    {
                        if ( returned >= data.size() )
                        {
                            throw new NoSuchElementException( "No more pairs to iterate." );
                        }
                        int basisIndex =
                                (int) Math.floor( ( (double) returned ) / getTimeStepCount() );
                        int residual = returned - ( basisIndex * getTimeStepCount() );
                        Instant left = getBasisTimes().get( basisIndex )
                                                      .plus( getRegularDuration().multipliedBy( (long) residual
                                                                                                + 1 ) );
                        Event<T> returnMe = Event.of( left, data.get( returned ) );
                        returned++;
                        return returnMe;
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

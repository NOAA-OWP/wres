package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.CrossPairMethod;
import wres.datamodel.pools.pairs.CrossPairs;
import wres.datamodel.pools.pairs.PairingException;
import wres.datamodel.time.TimeSeries.Builder;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Supports cross-pairing of two sets of {@link TimeSeries} by reference time and valid time.
 *
 * @param <T> the time-series event value type
 * @author James Brown
 */

public class TimeSeriesCrossPairer<T> implements BiFunction<List<TimeSeries<T>>, List<TimeSeries<T>>, CrossPairs<T>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesCrossPairer.class );

    /** Cross-pair method. */
    private final CrossPairMethod crossPair;

    /**
     * Creates an instance of a cross pairer using {@link wres.config.yaml.components.CrossPairMethod#FUZZY} matching by
     * reference time.
     *
     * @param <T> the time-series event value type
     * @return an instance
     */

    public static <T> TimeSeriesCrossPairer<T> of()
    {
        return new TimeSeriesCrossPairer<>( CrossPairMethod.FUZZY );
    }

    /**
     * Creates an instance of a cross pairer using a prescribed {@link wres.config.yaml.components.CrossPairMethod}.
     *
     * @param <T> the time-series event value type
     * @param crossPair the match mode for reference times
     * @return an instance
     * @throws NullPointerException if the match mode is null
     */

    public static <T> TimeSeriesCrossPairer<T> of( CrossPairMethod crossPair )
    {
        return new TimeSeriesCrossPairer<>( crossPair );
    }

    /**
     * Cross-pairs the two paired inputs.
     *
     * @param first the first list of time-series
     * @param second the second list of time-series to be cross-paired with the first
     * @return the cross pairs
     * @throws PairingException if the pairs could not be created
     * @throws NullPointerException if either input is null
     */

    @Override
    public CrossPairs<T> apply( List<TimeSeries<T>> first, List<TimeSeries<T>> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // Filter both sets of pairs
        List<TimeSeries<T>> filteredMain = this.crossPair( first, second );
        List<TimeSeries<T>> filteredBaseline = this.crossPair( second, filteredMain );

        // Log the pairs removed
        if ( LOGGER.isDebugEnabled() )
        {
            int firstTotal = first.stream()
                                  .mapToInt( a -> a.getEvents()
                                                   .size() )
                                  .sum();
            int secondTotal = second.stream()
                                    .mapToInt( a -> a.getEvents()
                                                     .size() )
                                    .sum();
            int firstFiltered = filteredMain.stream()
                                            .mapToInt( a -> a.getEvents()
                                                             .size() )
                                            .sum();
            int secondFiltered = filteredBaseline.stream()
                                                 .mapToInt( a -> a.getEvents()
                                                                  .size() )
                                                 .sum();

            LOGGER.debug( "Finished cross-pairing the first inputs, which contained {} pairs across {} time-series, "
                          + "with the second inputs, which contained {} pairs across {} time-series. Removed {} pairs "
                          + "across {} time-series from the first inputs and {} pairs across {} time-series from the "
                          + "second inputs.",
                          firstTotal,
                          first.size(),
                          secondTotal,
                          second.size(),
                          firstTotal - firstFiltered,
                          first.size(),
                          secondTotal - secondFiltered,
                          second.size() );
        }

        return CrossPairs.of( filteredMain, filteredBaseline );
    }

    /**
     * Create an instance.
     *
     * @param crossPair the match mode
     * @throws NullPointerException if the match mode is null
     */

    private TimeSeriesCrossPairer( CrossPairMethod crossPair )
    {
        Objects.requireNonNull( crossPair );

        this.crossPair = crossPair;

        LOGGER.debug( "Built a time-series cross-pairer with a matching mode of {}.", this.crossPair );
    }

    /**
     * Cross-pair the first input against the second.
     *
     * @param filterThese the pairs to be filtered
     * @param againstThese the pairs to filter against
     * @return the filtered pairs
     * @throws PairingException if the pairs could not be compared
     */

    private List<TimeSeries<T>> crossPair( List<TimeSeries<T>> filterThese,
                                           List<TimeSeries<T>> againstThese )
    {
        List<TimeSeries<T>> filterTheseMutable = new ArrayList<>( filterThese );
        List<TimeSeries<T>> returnMe = new ArrayList<>();

        // Iterate through the time-series to filter
        for ( TimeSeries<T> next : againstThese )
        {
            // Find the nearest time-series by reference time
            TimeSeries<T> nearest = this.getNearestByReferenceTimes( filterTheseMutable, next );

            Set<Instant> validTimesToCheck = next.getEvents()
                                                 .stream()
                                                 .map( Event::getTime )
                                                 .collect( Collectors.toSet() );

            SortedSet<Event<T>> events = nearest.getEvents()
                                                .stream()
                                                .filter( nextEvent -> validTimesToCheck.contains( nextEvent.getTime() ) )
                                                .collect( Collectors.toCollection( TreeSet::new ) );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "While cross-pairing a list of time-series with identity {} against another list of "
                              + "time-series with identity {}, inspected time-series {}, which contains {} events, and "
                              + "discovered the nearest time-series by reference time was {}, which contains {} "
                              + "events. Found {} events at common valid times, which were retained in the "
                              + "cross-paired time-series. These valid times were: {}",
                              filterThese.hashCode(),
                              againstThese.hashCode(),
                              next.getMetadata(),
                              next.getEvents().size(),
                              nearest.getMetadata(),
                              nearest.getEvents().size(),
                              events.size(),
                              events.stream()
                                    .map( Event::getTime )
                                    .collect( Collectors.toSet() ) );
            }

            if ( !events.isEmpty() )
            {
                // Consider only valid times that are part of the next time-series
                TimeSeries<T> nextSeries =
                        new Builder<T>().setMetadata( nearest.getMetadata() )
                                        .setEvents( events )
                                        .build();

                returnMe.add( nextSeries );

                // Consider one time-series only once
                filterTheseMutable.remove( nearest );
            }

        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Finds a time-series within the list of time-series whose reference times are nearest to those of the 
     * prescribed time-series. Nearest means the first time-series with identical reference times, else the first 
     * time-series whose common reference times differ by the minimum total duration (first if there are several such
     * cases).
     *
     * @param lookInHere the list in which to look
     * @param lookNearToMe the time-series whose reference times will be matched as closely as possible
     * @return the nearest time-series by reference times
     * @throws PairingException if there are no reference times in any one time-series
     */

    private TimeSeries<T> getNearestByReferenceTimes( List<TimeSeries<T>> lookInHere,
                                                      TimeSeries<T> lookNearToMe )
    {
        // Default to empty
        TimeSeries<T> nearest = null;
        Duration durationError = TimeWindowOuter.DURATION_MAX;

        Map<ReferenceTimeType, Instant> refTimesToCheck = lookNearToMe.getReferenceTimes();

        for ( TimeSeries<T> next : lookInHere )
        {
            // Equivalent reference times?
            if ( refTimesToCheck.equals( next.getReferenceTimes() ) )
            {
                return next;
            }

            // Find the approximate nearest
            // Find the total duration error for all reference times in the next series
            // relative to the series to check
            Duration nextError = this.getTotalDurationBetweenCommonTimeTypes( lookNearToMe,
                                                                              next );

            // If the existing nearest is null, this is the new nearest
            if ( Objects.isNull( nearest ) )
            {
                nearest = next;
                durationError = nextError;
                continue;
            }

            // Is it nearer than the current nearest?
            if ( nextError.compareTo( durationError ) < 0 )
            {
                nearest = next;
                durationError = nextError;
            }
        }

        // Return the empty time-series if nothing found or if the duration error is not zero when exact matching
        if ( Objects.isNull( nearest )
             || ( this.crossPair == CrossPairMethod.EXACT && !Duration.ZERO.equals( durationError ) ) )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug(
                        "While attempting to find a match by reference time for time-series {} within a list of {} "
                        + "  time-series, failed to identify a match. The match mode was {} and the total absolute "
                        + "duration between all considered reference times of the nearest time-series discovered was "
                        + "{}.",
                        lookNearToMe.getMetadata(),
                        lookInHere.size(),
                        this.crossPair,
                        durationError );
            }

            // Use the "near to me" metadata for the empty timeseries, though we
            // usually would want the "look in here" metadata. There may be no
            // timeseries in the latter and metadata is required in timeseries.
            return TimeSeries.of( lookNearToMe.getMetadata() );
        }

        return nearest;
    }

    /**
     * Returns the total absolute duration between the common reference time types in each input.
     *
     * @param first the first time-series
     * @param second the second time-series
     * @return the total absolute duration between the common times
     * @throws PairingException if there are no reference times in either input
     */

    private Duration getTotalDurationBetweenCommonTimeTypes( TimeSeries<T> first,
                                                             TimeSeries<T> second )
    {
        Map<ReferenceTimeType, Instant> firstTimes = first.getReferenceTimes();
        Map<ReferenceTimeType, Instant> secondTimes = second.getReferenceTimes();

        // Validate
        Set<ReferenceTimeType> common = new HashSet<>( firstTimes.keySet() );
        common.retainAll( secondTimes.keySet() );
        if ( firstTimes.isEmpty() || secondTimes.isEmpty() || common.isEmpty() )
        {
            throw new PairingException( "Encountered an error while inspecting time-series to cross-pair. Attempted to "
                                        + "calculate the total duration between the commonly typed "
                                        + "reference times of two time-series, but no commonly typed reference times "
                                        + "were discovered, which is not allowed. The first time-series was: "
                                        + first.getMetadata()
                                        + ". The second time-series was: "
                                        + second.getMetadata()
                                        + ". The first time-series had reference time types of: "
                                        + firstTimes.keySet()
                                        + ". The second time-series had reference time types of: "
                                        + secondTimes.keySet()
                                        + "." );
        }

        // The neutral difference
        Duration returnMe = Duration.ZERO;

        for ( Map.Entry<ReferenceTimeType, Instant> next : firstTimes.entrySet() )
        {
            ReferenceTimeType nextType = next.getKey();
            Instant nextInstant = next.getValue();

            if ( secondTimes.containsKey( nextType ) )
            {
                Instant otherValue = secondTimes.get( nextType );
                Duration difference = Duration.between( nextInstant, otherValue )
                                              .abs();
                returnMe = returnMe.plus( difference );
            }
        }

        return returnMe;
    }
}

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
 * @param <S> the time-series event value type in the first dataset
 * @param <T> the time-series event value type in the second dataset
 * @author James Brown
 */

public class TimeSeriesCrossPairer<S, T>
        implements BiFunction<List<TimeSeries<S>>, List<TimeSeries<T>>, CrossPairs<S, T>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesCrossPairer.class );

    /** Cross-pair method. */
    private final CrossPairMethod crossPair;

    /**
     * Creates an instance of a cross pairer using {@link wres.config.yaml.components.CrossPairMethod#FUZZY} matching by
     * reference time.
     *
     * @param <S> the time-series event value type for the first series
     * @param <T> the time-series event value type for the second series
     * @return an instance
     */

    public static <S, T> TimeSeriesCrossPairer<S, T> of()
    {
        return new TimeSeriesCrossPairer<>( CrossPairMethod.FUZZY );
    }

    /**
     * Creates an instance of a cross pairer using a prescribed {@link wres.config.yaml.components.CrossPairMethod}.
     *
     * @param <S> the time-series event value type for the first series
     * @param <T> the time-series event value type for the second series
     * @param crossPair the match mode for reference times
     * @return an instance
     * @throws NullPointerException if the match mode is null
     */

    public static <S, T> TimeSeriesCrossPairer<S, T> of( CrossPairMethod crossPair )
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
    public CrossPairs<S, T> apply( List<TimeSeries<S>> first, List<TimeSeries<T>> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // Sort the time-series in time order using the first reference time, if available
        List<TimeSeries<S>> firstSorted = new ArrayList<>( first );
        List<TimeSeries<T>> secondSorted = new ArrayList<>( second );

        // Filter both sets of pairs
        List<TimeSeries<S>> filteredMain = this.crossPair( firstSorted, secondSorted, this.crossPair );
        List<TimeSeries<T>> filteredBaseline = this.crossPair( secondSorted, filteredMain, this.crossPair );

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
     * @param <P> the type of data to filter
     * @param <Q> the type of data to filter against
     * @param filterThese the pairs to be filtered
     * @param againstThese the pairs to filter against
     * @param method the cross-pairing method
     * @return the filtered pairs
     * @throws PairingException if the pairs could not be compared
     */

    private <P, Q> List<TimeSeries<P>> crossPair( List<TimeSeries<P>> filterThese,
                                                  List<TimeSeries<Q>> againstThese,
                                                  CrossPairMethod method )
    {
        List<TimeSeries<P>> filterTheseMutable = new ArrayList<>( filterThese );
        List<TimeSeries<P>> returnMe = new ArrayList<>();

        // Iterate through the time-series to filter
        for ( TimeSeries<Q> next : againstThese )
        {
            // Find the nearest time-series by reference time
            TimeSeries<P> nearest =
                    this.getNearestByReferenceTimesWithMatchingValidTimes( filterTheseMutable, next, method );

            Set<Instant> validTimesToCheck = next.getEvents()
                                                 .stream()
                                                 .map( Event::getTime )
                                                 .collect( Collectors.toSet() );

            SortedSet<Event<P>> events = nearest.getEvents()
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

            // Some events to consider
            if ( !events.isEmpty() )
            {
                // Consider only valid times that are part of the next time-series
                TimeSeries<P> nextSeries =
                        new Builder<P>().setMetadata( nearest.getMetadata() )
                                        .setEvents( events )
                                        .build();

                returnMe.add( nextSeries );

                // Use one time-series only once
                filterTheseMutable.remove( nearest );
            }
            else
            {
                LOGGER.debug( "Eliminated a time-series from cross-pairing because no valid times matched." );
            }
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Finds a time-series within the list of time-series whose reference times are nearest to those of the 
     * prescribed time-series and at least some valid times match exactly. Nearest means the first time-series with
     * identical reference times, else the first time-series whose common reference times differ by the minimum total
     * duration (first if there are several such cases).
     *
     * @param <P> the type of data to be inspected
     * @param <Q> the type of data to match against
     * @param lookInHere the list in which to look
     * @param lookNearToMe the time-series whose reference times will be matched as closely as possible
     * @param method the cross-pairing method
     * @return the nearest time-series by reference times
     * @throws PairingException if there are no reference times in any one time-series
     */

    private <P, Q> TimeSeries<P> getNearestByReferenceTimesWithMatchingValidTimes( List<TimeSeries<P>> lookInHere,
                                                                                   TimeSeries<Q> lookNearToMe,
                                                                                   CrossPairMethod method )
    {
        // Default to empty
        TimeSeries<P> nearest = null;
        Duration durationError = TimeWindowOuter.DURATION_MAX;

        Map<ReferenceTimeType, Instant> refTimesToCheck = lookNearToMe.getReferenceTimes();

        Set<Instant> validTimesToCheck = lookNearToMe.getEvents()
                                                     .stream()
                                                     .map( Event::getTime )
                                                     .collect( Collectors.toSet() );

        for ( TimeSeries<P> next : lookInHere )
        {
            // Some common valid times?
            if ( next.getEvents()
                     .stream()
                     .anyMatch( e -> validTimesToCheck.contains( e.getTime() ) ) )
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
                                                                                  next,
                                                                                  method );

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
        }

        return this.getNearestOrEmpty( nearest, durationError, lookInHere, lookNearToMe );
    }

    /**
     * Returns the input time-series or an empty one if the input is null.
     *
     * @param <P> the type of data to filter
     * @param <Q> the type of data to filter against
     * @param nearest the nearest time-series to check
     * @param durationError the duration error
     * @param lookInHere the list in which to look
     * @param lookNearToMe the time-series whose reference times will be matched as closely as possible
     * @return the nearest time-series or any empty one
     */
    private <P, Q> TimeSeries<P> getNearestOrEmpty( TimeSeries<P> nearest,
                                                    Duration durationError,
                                                    List<TimeSeries<P>> lookInHere,
                                                    TimeSeries<Q> lookNearToMe )
    {

        // Return the empty time-series if nothing found or if the duration error is not zero when exact matching
        if ( Objects.isNull( nearest )
             || ( this.crossPair == CrossPairMethod.EXACT
                  && !Duration.ZERO.equals( durationError ) ) )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While attempting to find a match by reference time for time-series {} within a list of "
                              + "{} time-series, failed to identify a match. The match mode was {} and the total "
                              + "absolute duration between all considered reference times of the nearest time-series "
                              + "discovered was {}.",
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
     * @param method the cross-pairing method
     * @return the total absolute duration between the common times
     * @throws PairingException if there are no reference times in either input
     */

    private <P, Q> Duration getTotalDurationBetweenCommonTimeTypes( TimeSeries<P> first,
                                                                    TimeSeries<Q> second,
                                                                    CrossPairMethod method )
    {
        Map<ReferenceTimeType, Instant> firstTimes = first.getReferenceTimes();
        Map<ReferenceTimeType, Instant> secondTimes = second.getReferenceTimes();

        // Validate
        Set<ReferenceTimeType> common = new HashSet<>( firstTimes.keySet() );
        common.retainAll( secondTimes.keySet() );

        // Filter non-matching reference time types
        if ( method != CrossPairMethod.FUZZY )
        {
            firstTimes = firstTimes.entrySet()
                                   .stream()
                                   .filter( e -> common.contains( e.getKey() ) )
                                   .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
            secondTimes = secondTimes.entrySet()
                                     .stream()
                                     .filter( e -> common.contains( e.getKey() ) )
                                     .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        }

        if ( firstTimes.isEmpty() || secondTimes.isEmpty() )
        {
            String append = "";
            if ( method != CrossPairMethod.FUZZY )
            {
                append = "For lenient cross-pairing that considers all types of reference time equivalent, declare the "
                         + "'fuzzy' cross-pairing method. ";
            }

            throw new PairingException( "Encountered an error while inspecting time-series to cross-pair. Attempted to "
                                        + "calculate the total duration between the commonly typed "
                                        + "reference times of two time-series, but no commonly typed reference times "
                                        + "were discovered, which is not allowed. "
                                        + append
                                        + "The first time-series was: "
                                        + first.getMetadata()
                                        + ". The second time-series was: "
                                        + second.getMetadata()
                                        + ". The first time-series had reference time types of: "
                                        + first.getReferenceTimes()
                                               .keySet()
                                        + ". The second time-series had reference time types of: "
                                        + second.getReferenceTimes()
                                                .keySet()
                                        + "." );
        }

        // The neutral difference
        Duration returnMe = Duration.ZERO;

        for ( Instant firstInstant : firstTimes.values() )
        {
            for ( Instant secondInstant : secondTimes.values() )
            {
                Duration difference = Duration.between( firstInstant, secondInstant )
                                              .abs();
                returnMe = returnMe.plus( difference );
            }
        }

        return returnMe;
    }
}

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

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.pairs.CrossPairs;
import wres.datamodel.sampledata.pairs.PairingException;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Supports cross-pairing of two sets of paired time-series {@link TimeSeries} by reference time and valid time. 
 * 
 * @param <L> the left type of event value
 * @param <R> the right type of event value
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesCrossPairer<L, R>
        implements BiFunction<List<TimeSeries<Pair<L, R>>>, List<TimeSeries<Pair<L, R>>>, CrossPairs<L, R>>
{

    /**
     * An enumeration of techniques for matching by reference time.
     */

    public enum MatchMode
    {
        /**
         * Only pair time-series whose reference times match exactly.
         */

        EXACT,

        /**
         * Find the nearest time-series by reference time for each time-series considered, using each time-series only
         * once.
         */

        FUZZY;
    }

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesCrossPairer.class );

    /**
     * Match mode.
     */

    private final MatchMode matchMode;

    /**
     * Creates an instance of a cross pairer using {@link MatchMode#FUZZY} matching by reference time.
     * 
     * @param <L> the left type of data on one side of a pairing
     * @param <R> the right type of data on one side of a pairing
     * @return an instance
     */

    public static <L, R> TimeSeriesCrossPairer<L, R> of()
    {
        return new TimeSeriesCrossPairer<>( MatchMode.FUZZY );
    }

    /**
     * Creates an instance of a cross pairer using a prescribed {@link MatchMode}.
     * 
     * @param <L> the left type of data on one side of a pairing
     * @param <R> the right type of data on one side of a pairing
     * @param matchMode the match mode for reference times
     * @return an instance
     * @throws NullPointerException if the match mode is null
     */

    public static <L, R> TimeSeriesCrossPairer<L, R> of( MatchMode matchMode )
    {
        return new TimeSeriesCrossPairer<>( matchMode );
    }

    /**
     * Cross-pairs the two paired inputs.
     * 
     * @param mainPairs the pairs associated with the main dataset to be verified
     * @param baselinePairs the pairs associated with the baseline to be verified
     * @return the pairs
     * @throws PairingException if the pairs could not be created
     * @throws NullPointerException if either input is null
     */

    @Override
    public CrossPairs<L, R> apply( List<TimeSeries<Pair<L, R>>> mainPairs, List<TimeSeries<Pair<L, R>>> baselinePairs )
    {
        Objects.requireNonNull( mainPairs );
        Objects.requireNonNull( baselinePairs );

        // Filter both sets of pairs
        List<TimeSeries<Pair<L, R>>> filteredMain = this.crossPair( mainPairs, baselinePairs );
        List<TimeSeries<Pair<L, R>>> filteredBaseline = this.crossPair( baselinePairs, filteredMain );

        // Log the pairs removed
        if ( LOGGER.isDebugEnabled() )
        {
            int mainRemoved = mainPairs.size() - filteredMain.size();
            int baselineRemoved = baselinePairs.size() - filteredBaseline.size();

            LOGGER.debug( "Finished cross-pairing the right and baseline inputs, which removed {} pairs from the right "
                          + "inputs and {} pairs from the baseline inputs.",
                          mainRemoved,
                          baselineRemoved );
        }

        return CrossPairs.of( filteredMain, filteredBaseline );
    }

    /**
     * Create an instance.
     * 
     * @param matchMode the match mode
     * @throws NullPointerException if the match mode is null
     */

    private TimeSeriesCrossPairer( MatchMode matchMode )
    {
        Objects.requireNonNull( matchMode );

        this.matchMode = matchMode;

        LOGGER.debug( "Built a time-series cross-pairer with a matching mode of {}.", this.matchMode );
    }

    /**
     * Cross-pair the first input against the second.
     * 
     * @param filterThese the pairs to be filtered
     * @param againstThese the pairs to filter against
     * @return the filtered pairs
     * @throws PairingException if the pairs could not be compared
     */

    private List<TimeSeries<Pair<L, R>>> crossPair( List<TimeSeries<Pair<L, R>>> filterThese,
                                                    List<TimeSeries<Pair<L, R>>> againstThese )
    {
        List<TimeSeries<Pair<L, R>>> filterTheseMutable = new ArrayList<>( filterThese );
        List<TimeSeries<Pair<L, R>>> returnMe = new ArrayList<>();

        // Iterate through the time-series to filter
        for ( TimeSeries<Pair<L, R>> next : againstThese )
        {
            // Find the nearest time-series by reference time
            TimeSeries<Pair<L, R>> nearest = this.getNearestByReferenceTimes( filterTheseMutable, next );

            Set<Instant> validTimesToCheck = next.getEvents()
                                                 .stream()
                                                 .map( Event::getTime )
                                                 .collect( Collectors.toSet() );

            SortedSet<Event<Pair<L, R>>> events = nearest.getEvents()
                                                         .stream()
                                                         .filter( nextEvent -> validTimesToCheck.contains( nextEvent.getTime() ) )
                                                         .collect( Collectors.toCollection( TreeSet::new ) );

            if ( !events.isEmpty() )
            {
                // Consider only valid times that are part of the next time-series
                TimeSeries<Pair<L, R>> nextSeries =
                        new TimeSeriesBuilder<Pair<L, R>>().setMetadata( nearest.getMetadata() )
                                                           .addEvents( events )
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

    private TimeSeries<Pair<L, R>> getNearestByReferenceTimes( List<TimeSeries<Pair<L, R>>> lookInHere,
                                                               TimeSeries<Pair<L, R>> lookNearToMe )
    {
        // Default to empty
        TimeSeries<Pair<L, R>> nearest = null;
        Duration durationError = TimeWindow.DURATION_MAX;

        Map<ReferenceTimeType, Instant> refTimesToCheck = lookNearToMe.getReferenceTimes();

        for ( TimeSeries<Pair<L, R>> next : lookInHere )
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
             || ( this.matchMode == MatchMode.EXACT && !Duration.ZERO.equals( durationError ) ) )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While attempting to find a match by reference time for time-series {} within a list of {} "
                              + "  time-series, failed to identify a match. The match mode was {} and the total absolute "
                              + "duration between all considered reference times of the nearest time-series discovered was "
                              + "{}.",
                              lookNearToMe.getMetadata(),
                              lookInHere.size(),
                              this.matchMode,
                              durationError );
            }
            return TimeSeries.of();
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

    private Duration getTotalDurationBetweenCommonTimeTypes( TimeSeries<Pair<L, R>> first,
                                                             TimeSeries<Pair<L, R>> second )
    {
        Map<ReferenceTimeType, Instant> firstTimes = first.getReferenceTimes();
        Map<ReferenceTimeType, Instant> secondTimes = second.getReferenceTimes();

        // Validate
        Set<ReferenceTimeType> common = new HashSet<>( firstTimes.keySet() );
        common.retainAll( secondTimes.keySet() );
        if ( firstTimes.isEmpty() || secondTimes.isEmpty() || common.isEmpty() )
        {
            throw new PairingException( "While attempting to cross pair time-series " + first.getMetadata()
                                        + " against time-series "
                                        + second.getMetadata()
                                        + " using their common reference times by type, found no common reference time "
                                        + "types, which is not allowed." );
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

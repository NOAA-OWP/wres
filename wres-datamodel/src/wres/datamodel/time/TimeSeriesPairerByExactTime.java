package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.pairs.PairingException;
import wres.datamodel.scale.TimeScaleOuter;

/**
 * <p>Implements pairing of two {@link TimeSeries} by matching times exactly. The times considered may be reference
 * times and/or valid times, depending on a {@link TimePairingType} provided on construction. When the type is 
 * {@link TimePairingType#REFERENCE_TIME_AND_VALID_TIME}, then the reference times are considered. In that case, when 
 * pairing two time-series that both contain reference times, pairs are only formed when one or more reference times 
 * intersect. When constructed with {@link TimePairingType#VALID_TIME_ONLY} or one or both time-series do not contain 
 * reference times, then pairs are formed by valid time only. Exact matching means that times must be exactly equal.
 * 
 * <p>Can optionally add validation checks for admissible values on the left and right. If either the left or
 * right values are not admissible, no pair is added.
 * 
 * @param <L> the left type of event value
 * @param <R> the right type of event value
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesPairerByExactTime<L, R> implements TimeSeriesPairer<L, R>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesPairerByExactTime.class );

    /**
     * Admissible values on the left.
     */

    private final Predicate<L> leftAdmissibleValue;

    /**
     * Admissible values on the right.
     */

    private final Predicate<R> rightAdmissibleValue;

    /**
     * The times to inspect when looking for equivalence.
     */

    private final TimePairingType timePairingType;

    /**
     * Creates an instance. By default, when the left and right inputs both contain reference times, as shown by
     * {@link TimeSeries#getReferenceTimes()}, then the reference times associated with the right are preserved in the
     * paired outputs. If different behavior is desired, that must be additionally implemented.
     * 
     * @param <L> the left type of event value
     * @param <R> the right type of event value
     * @return an instance of the pairer
     */

    public static <L, R> TimeSeriesPairerByExactTime<L, R> of()
    {
        // Admit all values
        return new TimeSeriesPairerByExactTime<>( left -> true,
                                                  right -> true,
                                                  TimePairingType.REFERENCE_TIME_AND_VALID_TIME );
    }

    /**
     * Creates an instance with left and right value guards. See also {@link #of()}.
     * 
     * @param <L> the left type of event value
     * @param <R> the right type of event value
     * @param leftAdmissibleValue the left value guard
     * @param rightAdmissibleValue the right value guard
     * @return an instance of the pairer
     * @throws NullPointerException if either input is null
     */

    public static <L, R> TimeSeriesPairerByExactTime<L, R> of( Predicate<L> leftAdmissibleValue,
                                                               Predicate<R> rightAdmissibleValue )
    {
        return new TimeSeriesPairerByExactTime<>( leftAdmissibleValue,
                                                  rightAdmissibleValue,
                                                  TimePairingType.REFERENCE_TIME_AND_VALID_TIME );
    }

    /**
     * Creates an instance with left and right value guards and an explicit pairing type.
     * 
     * @param <L> the left type of event value
     * @param <R> the right type of event value
     * @param leftAdmissibleValue the left value guard
     * @param rightAdmissibleValue the right value guard
     * @param timePairingType the time pairing type 
     * @return an instance of the pairer
     * @throws NullPointerException if any input is null
     */

    public static <L, R> TimeSeriesPairerByExactTime<L, R> of( Predicate<L> leftAdmissibleValue,
                                                               Predicate<R> rightAdmissibleValue,
                                                               TimePairingType timePairingType )
    {
        return new TimeSeriesPairerByExactTime<>( leftAdmissibleValue,
                                                  rightAdmissibleValue,
                                                  timePairingType );
    }

    @Override
    public TimeSeries<Pair<L, R>> pair( TimeSeries<L> left, TimeSeries<R> right )
    {
        Objects.requireNonNull( left, "Cannot pair a left time-series that is null." );
        Objects.requireNonNull( right, "Cannot pair a right time-series that is null." );

        this.validateTimeScalesForPairing( left, right );

        // Any reference times on one side only or that intersect in both type and value?
        Map<ReferenceTimeType, Instant> referenceTimes = this.getLeftOrRightOrIntersectingReferenceTimes( left, right );

        // If pairing by reference time and both have reference times and none intersect, then there are no pairs
        if ( this.getTimePairingType() == TimePairingType.REFERENCE_TIME_AND_VALID_TIME
             && referenceTimes.isEmpty()
             && !left.getReferenceTimes().isEmpty()
             && !right.getReferenceTimes().isEmpty() )
        {
            LOGGER.debug( "While attempting to pair left time-series {} with right time-series {} using a time-based "
                          + "pairing strategy of {}, discovered no intersecting reference times in the two time series "
                          + "and hence no pairs.",
                          left.hashCode(),
                          right.hashCode(),
                          TimePairingType.REFERENCE_TIME_AND_VALID_TIME );

            return new TimeSeriesBuilder<Pair<L, R>>().setMetadata( right.getMetadata() )
                                                      .build();
        }

        Map<Instant, Event<L>> mapper = new TreeMap<>();

        // Add the left by valid time
        left.getEvents().forEach( next -> mapper.put( next.getTime(), next ) );

        // The pairs
        SortedSet<Event<Pair<L, R>>> pairs = new TreeSet<>();

        int leftInadmissible = 0;
        int rightInadmissible = 0;

        // Find the right with corresponding valid times to the left
        for ( Event<R> nextRight : right.getEvents() )
        {
            // Right value admissible and right time present in map?
            if ( this.rightAdmissibleValue.test( nextRight.getValue() ) )
            {
                if ( mapper.containsKey( nextRight.getTime() ) )
                {
                    Event<L> nextLeft = mapper.get( nextRight.getTime() );

                    // Left value admissible?
                    if ( this.leftAdmissibleValue.test( nextLeft.getValue() ) )
                    {
                        pairs.add( Event.of( nextLeft.getTime(),
                                             Pair.of( nextLeft.getValue(), nextRight.getValue() ) ) );
                    }
                    else
                    {
                        leftInadmissible++;
                    }
                }
            }
            else
            {
                rightInadmissible++;
            }
        }

        // Log inadmissible cases
        this.logInadmissibleCases( left, right, leftInadmissible, rightInadmissible );

        TimeSeriesMetadata metadata =
                new TimeSeriesMetadata.Builder( right.getMetadata() ).setReferenceTimes( referenceTimes )
                                                                     .build();

        return new TimeSeriesBuilder<Pair<L, R>>().setMetadata( metadata )
                                                  .addEvents( pairs )
                                                  .build();
    }

    /**
     * Validates the time-scale information for pairing.
     * 
     * @param left the left series
     * @param right the right series
     * @throws PairingException if the scales are inconsistent
     */

    private void validateTimeScalesForPairing( TimeSeries<L> left, TimeSeries<R> right )
    {
        if ( left.hasTimeScale() && right.hasTimeScale()
             && !this.timeScalesAreEqual( left.getTimeScale(), right.getTimeScale() ) )
        {
            throw new PairingException( "Cannot pair two datasets with different time scales. The left time-series "
                                        + "has a time-scale of '"
                                        + left.getTimeScale()
                                        + "' and the right time-series has a time-scale of '"
                                        + right.getTimeScale()
                                        + "'." );
        }

        if ( !left.hasTimeScale() && LOGGER.isTraceEnabled() )
        {
            String add = "";
            if ( right.hasTimeScale() )
            {
                add = " which has time scale " + right.getTimeScale().toString() + ",";
            }

            LOGGER.trace( "While attempting to pair left time-series {} with right time-series {},{} discovered that "
                          + "the left time-series has missing time scale information. Proceeding and assuming that the "
                          + "left and right time-series have equivalent time scales.",
                          left.hashCode(),
                          right.hashCode(),
                          add );
        }

        if ( !right.hasTimeScale() && LOGGER.isTraceEnabled() )
        {
            String add = "";
            if ( left.hasTimeScale() )
            {
                add = ", which has time scale " + left.getTimeScale().toString() + ", ";
            }

            LOGGER.trace( "While attempting to pair left time-series {}{} with right time-series {}, discovered that "
                          + "the right time-series has missing time scale information. Proceeding and assuming that "
                          + "the left and right time-series have equivalent time scales.",
                          left.hashCode(),
                          right.hashCode(),
                          add );
        }
    }

    /**
     * Returns <code>true</code> if the inputs are equal, otherwise <code>false</code>.
     * 
     * @param left the left time scale
     * @param right the right time scale 
     * @return true if the time scales are equal, otherwise false
     */

    private boolean timeScalesAreEqual( TimeScaleOuter left, TimeScaleOuter right )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );

        if ( left.isInstantaneous() && right.isInstantaneous() )
        {
            return true;
        }

        return left.equals( right );
    }

    /**
     * Returns:
     * 
     * <ol>
     * <li>The reference times present in the left if the right contains no reference times</li>
     * <li>The reference times present in the right if the left contains no reference times</li>
     * <li>The reference times present in both the left and right if the {@link #timePairingType} is 
     * {@link TimePairingType#VALID_TIME_ONLY}</li>
     * <li>The intersecting reference times by type that are present in the right and the left if the 
     * {@link #timePairingType} is {@link TimePairingType#REFERENCE_TIME_AND_VALID_TIME}</li> </li>
     * </ol>
     * 
     * @return the left or right or intersecting reference times
     */

    private Map<ReferenceTimeType, Instant> getLeftOrRightOrIntersectingReferenceTimes( TimeSeries<L> left,
                                                                                        TimeSeries<R> right )
    {
        // One or both sides have no reference times
        if ( left.getReferenceTimes().isEmpty() )
        {
            return right.getReferenceTimes();
        }
        else if ( right.getReferenceTimes().isEmpty() )
        {
            return left.getReferenceTimes();
        }

        // Comparing by valid time only, return the union of reference times
        if ( this.getTimePairingType() == TimePairingType.VALID_TIME_ONLY )
        {
            Map<ReferenceTimeType, Instant> returnMe = new EnumMap<>( ReferenceTimeType.class );
            returnMe.putAll( left.getReferenceTimes() );
            returnMe.putAll( right.getReferenceTimes() );

            return Collections.unmodifiableMap( returnMe );
        }

        // Find the intersecting types
        Set<ReferenceTimeType> retained = new HashSet<>( left.getReferenceTimes().keySet() );
        retained.retainAll( right.getReferenceTimes().keySet() );
        if ( !retained.isEmpty() )
        {

            Map<ReferenceTimeType, Instant> returnMe = new EnumMap<>( ReferenceTimeType.class );

            // Any match?
            for ( ReferenceTimeType nextType : retained )
            {
                Instant leftRef = left.getReferenceTimes().get( nextType );
                Instant rightRef = right.getReferenceTimes().get( nextType );

                if ( leftRef.equals( rightRef ) )
                {
                    returnMe.put( nextType, leftRef );
                }
            }

            return Collections.unmodifiableMap( returnMe );
        }

        return Collections.emptyMap();
    }

    /**
     * Logs information about inadmissible pairs.
     * 
     * @param left the left time-series
     * @param right the right time-series
     * @param leftInadmissible the number of pairs that were inadmissible on left values
     * @param rightInadmissible the number of pairs that were inadmissible on right values
     * 
     */

    private void logInadmissibleCases( TimeSeries<L> left,
                                       TimeSeries<R> right,
                                       int leftInadmissible,
                                       int rightInadmissible )
    {

        // Log inadmissible cases
        if ( LOGGER.isDebugEnabled() && ( leftInadmissible > 0 || rightInadmissible > 0 ) )
        {
            LOGGER.debug( "While pairing left time-series {} with right time-series {}, found {} of {} left values that"
                          + " were inadmissible and {} of {} right values that were inadmissible.",
                          left.hashCode(),
                          right.hashCode(),
                          leftInadmissible,
                          left.getEvents().size(),
                          rightInadmissible,
                          right.getEvents().size() );
        }
    }

    /**
     * Returns the {@link TimePairingType} associated with the instance.
     * 
     * @return the time pairing type.
     */

    private TimePairingType getTimePairingType()
    {
        return this.timePairingType;
    }

    /**
     * Hidden constructor.
     * 
     * @param leftAdmissibleValue the left value guard
     * @param rightAdmissibleValue the right value guard
     * @throws NullPointerException if any input is null
     */

    private TimeSeriesPairerByExactTime( Predicate<L> leftAdmissibleValue,
                                         Predicate<R> rightAdmissibleValue,
                                         TimePairingType timePairingType )
    {
        Objects.requireNonNull( leftAdmissibleValue );
        Objects.requireNonNull( rightAdmissibleValue );
        Objects.requireNonNull( timePairingType );

        this.leftAdmissibleValue = leftAdmissibleValue;
        this.rightAdmissibleValue = rightAdmissibleValue;
        this.timePairingType = timePairingType;

        LOGGER.debug( "Built a time-based pairer that considers the time information '{}.'",
                      this.getTimePairingType() );

    }

}

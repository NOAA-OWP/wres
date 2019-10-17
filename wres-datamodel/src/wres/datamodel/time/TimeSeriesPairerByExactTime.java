package wres.datamodel.time;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.pairs.PairingException;

/**
 * <p>Implements pairing of two {@link TimeSeries} by valid time with exact matching. In other words, pairs are created
 * for each corresponding {@link Instant} in the left and right inputs. Additionally, when the left and right inputs 
 * both contain reference times, as shown by {@link TimeSeries#getReferenceTimes()}, then the reference times associated 
 * with the right input are preserved in the pairs and those associated with the left are discarded. Different behavior
 * should be implemented in different implementations of {@link TimeSeriesPairer}.
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
        return new TimeSeriesPairerByExactTime<>( left -> true, right -> true );
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
        return new TimeSeriesPairerByExactTime<>( leftAdmissibleValue, rightAdmissibleValue );
    }

    @Override
    public TimeSeries<Pair<L, R>> pair( TimeSeries<L> left, TimeSeries<R> right )
    {
        Objects.requireNonNull( left, "Cannot pair a left time-series that is null." );
        Objects.requireNonNull( right, "Cannot pair a right time-series that is null." );

        this.validateTimeScalesForPairing( left, right );

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
        if ( LOGGER.isTraceEnabled() && ( leftInadmissible > 0 || rightInadmissible > 0 ) )
        {
            LOGGER.trace( "While pairing left time-series {} with right time-series {}, found {} of {} left values that"
                          + " were inadmissible and {} of {} right values that were inadmissible.",
                          left.hashCode(),
                          right.hashCode(),
                          leftInadmissible,
                          left.getEvents().size(),
                          rightInadmissible,
                          right.getEvents().size() );
        }

        return new TimeSeriesBuilder<Pair<L, R>>().addReferenceTimes( right.getReferenceTimes() )
                                                  .addEvents( pairs )
                                                  .setTimeScale( left.getTimeScale() )
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
        if ( left.hasTimeScale() && right.hasTimeScale() && !left.getTimeScale().equals( right.getTimeScale() ) )
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
     * Hidden constructor.
     * 
     * @param leftAdmissibleValue the left value guard
     * @param rightAdmissibleValue the right value guard
     * @throws NullPointerException if either input is null
     */

    private TimeSeriesPairerByExactTime( Predicate<L> leftAdmissibleValue, Predicate<R> rightAdmissibleValue )
    {
        Objects.requireNonNull( leftAdmissibleValue );
        Objects.requireNonNull( rightAdmissibleValue );

        this.leftAdmissibleValue = leftAdmissibleValue;
        this.rightAdmissibleValue = rightAdmissibleValue;
    }

}

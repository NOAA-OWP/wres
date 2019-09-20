package wres.datamodel.time;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.sampledata.pairs.PairingException;

/**
 * Implements pairing of two {@link TimeSeries} by valid time with exact matching. In other words, pairs are created
 * for each corresponding {@link Instant} in the left and right inputs. Additionally, when the left and right inputs 
 * both contain reference times, as shown by {@link TimeSeries#getReferenceTimes()}, then the reference times associated 
 * with the right input are preserved in the pairs and those associated with the left are discarded. Different behavior
 * should be implemented in different implementations of {@link TimeSeriesPairer}.
 * 
 * @param <L> the left type of event value
 * @param <R> the right type of event value
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesPairerByExactValidTime<L, R> implements TimeSeriesPairer<L, R>
{

    /**
     * Creates an instance. By default, when the left and right inputs both contain reference times, as shown by
     * {@link TimeSeries#getReferenceTimes()}, then the reference times associated with the right are preserved in the
     * paired outputs. If different behavior is desired, that must be additionally implemented.
     * 
     * @param <L> the left type of event value
     * @param <R> the right type of event value
     * @return an instance of the pairer
     */

    public static <L, R> TimeSeriesPairerByExactValidTime<L, R> of()
    {
        return new TimeSeriesPairerByExactValidTime<>();
    }


    @Override
    public TimeSeries<Pair<L, R>> pair( TimeSeries<L> left, TimeSeries<R> right )
    {
        Objects.requireNonNull( left, "Cannot pair a left time-series that is null." );
        Objects.requireNonNull( right, "Cannot pair a right time-series that is null." );

        if ( !left.getTimeScale().equals( right.getTimeScale() ) )
        {
            throw new PairingException( "Cannot pair two datasets with different time scales. The left time-series "
                                        + "has a time-scale of '"
                                        + left.getTimeScale()
                                        + "' and the right time-series has a time-scale of '"
                                        + right.getTimeScale()
                                        + "'." );
        }

        Map<Instant, Event<L>> mapper = new TreeMap<>();

        // Add the left by valid time
        left.getEvents().forEach( next -> mapper.put( next.getTime(), next ) );

        // The pairs
        SortedSet<Event<Pair<L, R>>> pairs = new TreeSet<>();

        // Find the right with corresponding valid times to the left
        for ( Event<R> nextRight : right.getEvents() )
        {
            if ( mapper.containsKey( nextRight.getTime() ) )
            {
                Event<L> nextLeft = mapper.get( nextRight.getTime() );
                pairs.add( Event.of( nextLeft.getTime(), Pair.of( nextLeft.getValue(), nextRight.getValue() ) ) );
            }
        }

        return TimeSeries.of( right.getReferenceTimes(), pairs );
    }

    /**
     * Hidden constructor.
     */

    private TimeSeriesPairerByExactValidTime()
    {
    }

}

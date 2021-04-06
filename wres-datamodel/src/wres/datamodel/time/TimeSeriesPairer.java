package wres.datamodel.time;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.pools.pairs.PairingException;

/**
 * Supports pairing of a left and right {@link TimeSeries} by valid time. 
 * 
 * @param <L> the left type of event value
 * @param <R> the right type of event value
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesPairer<L, R>
{
    /**
     * The type of times to consider when conducting time-based pairing.
     */

    enum TimePairingType
    {
        /**
         * Only consider valid times.
         */
        
        VALID_TIME_ONLY,
        
        /**
         * Consider both valid times and, where available, reference times.
         */
        
        REFERENCE_TIME_AND_VALID_TIME,
    }

    /**
     * Pairs the left and right inputs.
     * 
     * @param left the left series
     * @param right the right series
     * @return the pairs
     * @throws PairingException if the pairs could not be created
     * @throws NullPointerException if either input is null
     */

    TimeSeries<Pair<L, R>> pair( TimeSeries<L> left, TimeSeries<R> right );

}

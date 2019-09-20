package wres.datamodel.time;

import org.apache.commons.lang3.tuple.Pair;
import wres.datamodel.sampledata.pairs.PairingException;

/**
 * Supports pairing of a left and right {@link TimeSeries} by valid time. 
 * 
 * @param <L> the left type of event value
 * @param <R> the right type of event value
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesPairer<L,R>
{

    /**
     * Pairs the left and right inputs.
     * 
     * @param left the left series
     * @param right the right series
     * @return the pairs
     * @throws PairingException if the pairs could not be created
     * @throws NullPointerException if either input is null
     */
    
    TimeSeries<Pair<L,R>> pair( TimeSeries<L> left, TimeSeries<R> right );  
    
}

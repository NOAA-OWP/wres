package wres.datamodel.time;

import java.util.List;
import java.util.function.BiFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.pairs.CrossPairs;
import wres.datamodel.sampledata.pairs.PairingException;

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
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesCrossPairer.class );

    /**
     * Creates an instance of a cross pairer.
     * 
     * @param <L> the left type of data on one side of a pairing
     * @param <R> the right type of data on one side of a pairing
     * @return an instance
     */

    public static <L, R> TimeSeriesCrossPairer<L, R> of()
    {
        return new TimeSeriesCrossPairer<>();
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
        int mainRemoved = 0;
        int baselineRemoved = 0;

        LOGGER.debug( "Finished cross-pairing the right and baseline inputs, which removed {} pairs from the right "
                      + "inputs and {} pairs from the baseline inputs.",
                      mainRemoved,
                      baselineRemoved );

        return CrossPairs.of( mainPairs, baselinePairs );
    }

    /**
     * Create an instance.
     */

    private TimeSeriesCrossPairer()
    {
    }

}

package wres.datamodel.inputs.pairs.builders;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A builder for a regular {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

public interface RegularTimeSeriesOfSingleValuedPairsBuilder extends PairedInputBuilder<PairOfDoubles>
{

    /**
     * Adds an atomic time-series to the builder.  The values must be time-ordered, moving away from the basis time.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the time-series values, ordered from earliest to latest
     * @return the builder
     */

    default RegularTimeSeriesOfSingleValuedPairsBuilder addData( Instant basisTime,
                                                                 List<PairOfDoubles> values )
    {
        Map<Instant, List<PairOfDoubles>> input = new HashMap<>();
        input.put( basisTime, values );
        return addData( input );
    }

    /**
     * Adds an atomic time-series to the builder for a baseline.  The values must be time-ordered, moving away from 
     * the basis time.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the time-series values, ordered from earliest to latest
     * @return the builder
     */

    default RegularTimeSeriesOfSingleValuedPairsBuilder addDataForBaseline( Instant basisTime,
                                                                            List<PairOfDoubles> values )
    {
        Map<Instant, List<PairOfDoubles>> input = new HashMap<>();
        input.put( basisTime, values );
        return addDataForBaseline( input );
    }

    /**
     * Adds several time-series to the builder, each one stored against its basis time.  The values must be time-
     * ordered, moving away from the basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    RegularTimeSeriesOfSingleValuedPairsBuilder addData( Map<Instant, List<PairOfDoubles>> timeSeries );

    /**
     * Adds several time-series to the builder for a baseline, each one stored against its basis time. The values must
     * be time-ordered, moving away from the basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    RegularTimeSeriesOfSingleValuedPairsBuilder addDataForBaseline( Map<Instant, List<PairOfDoubles>> timeSeries );

    /**
     * Adds a time-series to the builder.
     * 
     * @param timeSeries the regular time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     */

    RegularTimeSeriesOfSingleValuedPairsBuilder addTimeSeries( TimeSeriesOfSingleValuedPairs timeSeries );

    /**
     * Sets the time-step of the regular time-series.
     * 
     * @param timeStep the time-step of the regular time-series
     * @return the builder
     */

    RegularTimeSeriesOfSingleValuedPairsBuilder setTimeStep( Duration timeStep );

    /**
     * Builds a time-series.
     * 
     * @return a time-series
     */

    TimeSeriesOfSingleValuedPairs build();

}

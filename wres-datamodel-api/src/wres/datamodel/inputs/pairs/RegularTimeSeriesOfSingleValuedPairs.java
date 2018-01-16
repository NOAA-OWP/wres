package wres.datamodel.inputs.pairs;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A regular {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public interface RegularTimeSeriesOfSingleValuedPairs extends TimeSeriesOfSingleValuedPairs
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined. 
     * 
     * @return the baseline
     */

    RegularTimeSeriesOfSingleValuedPairs getBaselineData();

    /**
     * A builder to build the time-series.
     */

    interface RegularTimeSeriesOfSingleValuedPairsBuilder extends PairedInputBuilder<PairOfDoubles>
    {

        /**
         * Adds an atomic time-series to the builder. If the basis time already exists, the values are appended 
         * (i.e. are assumed to represent later values). 
         * 
         * @param basisTime the basis time for the time-series
         * @param values the time-series values, ordered from earliest to latest
         * @return the builder
         */

        RegularTimeSeriesOfSingleValuedPairsBuilder addData( Instant basisTime,
                                                             List<PairOfDoubles> values );

        /**
         * Adds an atomic time-series to the builder for a baseline. If the basis time already exists, the values are 
         * appended (i.e. are assumed to represent later values). 
         * 
         * @param basisTime the basis time for the time-series
         * @param values the time-series values, ordered from earliest to latest
         * @return the builder
         */

        RegularTimeSeriesOfSingleValuedPairsBuilder addDataForBaseline( Instant basisTime,
                                                                        List<PairOfDoubles> values );

        /**
         * Adds a regular time-series to the builder.
         * 
         * @param timeSeries the regular time-series
         * @return the builder
         * @throws MetricInputException if the specified input is inconsistent with any existing input
         */

        RegularTimeSeriesOfSingleValuedPairsBuilder addTimeSeries( RegularTimeSeriesOfSingleValuedPairs timeSeries );

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

        RegularTimeSeriesOfSingleValuedPairs build();
    }


}

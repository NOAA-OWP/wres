package wres.datamodel.inputs.pairs;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public interface TimeSeriesOfSingleValuedPairs extends SingleValuedPairs, TimeSeries<PairOfDoubles>
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined. 
     * 
     * @return the baseline
     */

    TimeSeriesOfSingleValuedPairs getBaselineData();

}

package wres.datamodel.inputs.pairs;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.time.TimeSeries;

/**
 * A regular {@link TimeSeries} of {@link SingleValuedPairs}
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public interface RegularTimeSeriesOfSingleValuedPairs extends SingleValuedPairs, TimeSeries<PairOfDoubles>
{
    
    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined. 
     * 
     * @return the baseline
     */
    
    RegularTimeSeriesOfSingleValuedPairs getBaselineData();
    
}

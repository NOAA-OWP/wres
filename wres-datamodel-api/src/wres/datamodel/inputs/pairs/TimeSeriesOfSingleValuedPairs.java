package wres.datamodel.inputs.pairs;

import wres.datamodel.time.TimeSeries;

/**
 * <p>A {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface TimeSeriesOfSingleValuedPairs extends SingleValuedPairs, TimeSeries<PairOfDoubles>
{
    
    @Override
    TimeSeriesOfSingleValuedPairs getBaselineData();

}

package wres.datamodel.inputs.pairs;

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

    @Override
    TimeSeriesOfSingleValuedPairs getBaselineData();

}

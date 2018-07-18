package wres.datamodel.inputs.pairs;

import wres.datamodel.time.TimeSeries;

/**
 * <p>A {@link TimeSeries} of {@link EnsemblePairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface TimeSeriesOfEnsemblePairs extends EnsemblePairs, TimeSeries<PairOfDoubleAndVectorOfDoubles>
{

    @Override
    TimeSeriesOfEnsemblePairs getBaselineData();

}

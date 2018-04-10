package wres.datamodel.inputs.pairs;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesOfEnsemble;

/**
 * <p>A {@link TimeSeries} of {@link EnsemblePairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface TimeSeriesOfEnsemblePairs
        extends EnsemblePairs, TimeSeriesOfEnsemble<PairOfDoubles, PairOfDoubleAndVectorOfDoubles>
{
    
    @Override
    TimeSeriesOfEnsemblePairs getBaselineData();

}

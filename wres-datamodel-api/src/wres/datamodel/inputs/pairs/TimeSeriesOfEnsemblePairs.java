package wres.datamodel.inputs.pairs;

import java.util.function.Predicate;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesOfEnsemble;

/**
 * <p>A {@link TimeSeries} of {@link EnsemblePairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public interface TimeSeriesOfEnsemblePairs
        extends EnsemblePairs, TimeSeriesOfEnsemble<PairOfDoubles, PairOfDoubleAndVectorOfDoubles>
{

    @Override
    TimeSeriesOfEnsemblePairs getBaselineData();

    @Override
    TimeSeriesOfEnsemblePairs filterByTraceIndex( Predicate<Integer> traceFilter );

}

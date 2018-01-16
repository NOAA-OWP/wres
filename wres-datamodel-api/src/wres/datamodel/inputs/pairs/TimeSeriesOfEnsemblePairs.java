package wres.datamodel.inputs.pairs;

import java.util.function.Predicate;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A {@link TimeSeries} of {@link EnsemblePairs}.</p>
 * 
 * <p>Every time-series in the container must contain the same number of ensemble members, thereby allowing for 
 * iteration by ensemble trace.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public interface TimeSeriesOfEnsemblePairs extends EnsemblePairs, TimeSeries<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * Returns a view of each ensemble trace as a {@link TimeSeriesOfSingleValuedPairs}. The atomic time-series 
     * are returned in trace order. Baseline data is not added to the trace view, because there is no guaranteed 
     * connection between the trace views of the main dataset and the baseline dataset (e.g. they may contain a 
     * different number of ensemble members).
     * 
     * @return an iterable view of the atomic time-series by ensemble trace, without any baseline data
     */

    Iterable<TimeSeriesOfSingleValuedPairs> ensembleTraceIterator();
    
    /**
     * Returns a {@link TimeSeries} whose elements are filtered according to the zero-based index of the ensemble trace 
     * or null if no such time-series exist.
     * 
     * @param traceFilter the trace index filter
     * @return a time-series associated with a specific trace or null
     */

    TimeSeriesOfEnsemblePairs filterByTraceIndex( Predicate<Integer> traceFilter );    

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined. 
     * 
     * @return the baseline
     */

    TimeSeriesOfEnsemblePairs getBaselineData();

}

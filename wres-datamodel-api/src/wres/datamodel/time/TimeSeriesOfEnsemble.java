package wres.datamodel.time;

/**
 * <p>A {@link TimeSeries} that is composed as an ensemble and may be decomposed into ensemble traces.</p>
 * 
 * <p>Every time-series in the container must contain the same number of ensemble members, thereby allowing for 
 * iteration by ensemble trace.</p>
 * 
 * @param <S> the trace-decomposed type of data
 * @param <T> the trace-composed type of data
 * @author james.brown@hydrosolved.com
 */
public interface TimeSeriesOfEnsemble<S,T> extends TimeSeries<T>
{

    /**
     * Returns a view of each ensemble trace. The atomic time-series are returned in trace order. Baseline data is not 
     * added to the trace view, because there is no guaranteed connection between the trace views of the main dataset 
     * and the baseline dataset (e.g. they may contain a different number of ensemble members).
     * 
     * @return an iterable view of the atomic time-series by ensemble trace, without any baseline data
     */

    Iterable<TimeSeries<S>> ensembleTraceIterator();

}

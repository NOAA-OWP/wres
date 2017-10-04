package wres.datamodel;

import java.util.List;

/**
 * A list of pairs to be iterated over by a metric. 
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface PairedInput<S> extends MetricInput<List<S>>, Iterable<S>
{

}

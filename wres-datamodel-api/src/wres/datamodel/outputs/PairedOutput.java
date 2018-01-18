package wres.datamodel.outputs;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/**
 * A metric output that comprises a list of pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

public interface PairedOutput<S,T> extends MetricOutput<List<Pair<S,T>>>, Iterable<Pair<S,T>>
{    
}

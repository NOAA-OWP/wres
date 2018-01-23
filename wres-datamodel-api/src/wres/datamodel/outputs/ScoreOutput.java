package wres.datamodel.outputs;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;

/**
 * An output associated with a score. A score may contain one or more components, such as the components of a score
 * decomposition or the values associated with alternative methods of calculation. 
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */

public interface ScoreOutput<T> extends MetricOutput<T>, Iterable<Pair<MetricConstants, T>>
{

    /**
     * Returns the value associated with a prescribed {@link MetricConstants} in the input.
     * 
     * @param component the component required
     * @return the scalar value associated with the component or null if the component does not exist
     */

    T getValue( MetricConstants component );

    /**
     * Returns <code>true</code> if the score has the component specified, <code>false</code> otherwise.
     * 
     * @param component the component to test
     * @return true if the score component exists in this context, false otherwise
     */

    boolean hasComponent( MetricConstants component );

    /**
     * Returns the set of components for which score results exist.
     * 
     * @return the set of components with results
     */

    Set<MetricConstants> getComponents();

}

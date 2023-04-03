package wres.datamodel.statistics;

import java.util.Set;

import wres.config.MetricConstants;
import wres.datamodel.statistics.ScoreStatistic.ScoreComponent;

/**
 * A score statistic. A score may contain one or more components, such as the components of a score
 * decomposition or the values associated with alternative methods of calculation. A score is a mapping between 
 * {@link MetricConstants} and score values.
 * 
 * @param <T> the raw type of the score
 * @param <U> the type of score component
 * @author James Brown
 */

public interface ScoreStatistic<T, U extends ScoreComponent<?>> extends Statistic<T>, Iterable<U>
{
    /**
     * Returns the value associated with a prescribed {@link MetricConstants} in the input.
     * 
     * @param component the component required
     * @return the score associated with the component
     * @throws IllegalArgumentException if the component does not exist
     */

    U getComponent( MetricConstants component );

    /**
     * Returns the set of components for which score results exist.
     * 
     * @return the set of components with results
     */

    Set<MetricConstants> getComponents();

    /**
     * Returns the score statistic as a raw type.
     * 
     * @return the score.
     */
    @Override
    T getData();

    /**
     * Abstractions of a score component.
     * 
     * @param <S> the type of component value
     * @author James Brown
     */
    
    interface ScoreComponent<S> extends Statistic<S>
    {
    }
    
}

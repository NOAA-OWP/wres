package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants.MetricDecompositionGroup;

/**
 * Identifies a scoring rule. A score may be an absolute or relative measure. An absolute score is dimensioned, whereas
 * a relative scores is dimensionless. A relative score is known as a skill score. Some scores may be factored into
 * components. Supported decompositions and elements of decompositions are identified by the constants within this
 * class.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

interface Score
{

    /**
     * Returns true if the score is a relative measure or skill score, false for an absolute measure.
     * 
     * @return true if the score is a skill score
     */

    boolean isSkillScore();

    /**
     * Returns true if the score is decomposable, false otherwise.
     * 
     * @return true if the score is decomposable, false otherwise
     */

    boolean isDecomposable();

    /**
     * Returns the template associated with the decomposition. Must return {@link MetricDecompositionGroup#NONE} when 
     * {@link #isDecomposable()} returns false.
     * 
     * @return the type of score decomposition
     */

    MetricDecompositionGroup getDecompositionID();

}

package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.SampleData;
import wres.datamodel.statistics.ScoreStatistic;

/**
 * Identifies a scoring rule. A score may be an absolute or relative measure. An absolute score is dimensioned, whereas
 * a relative scores is dimensionless. A relative score is known as a skill score. Some scores may be factored into
 * components. Supported decompositions and elements of decompositions are identified by the constants within this
 * class.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface Score<S extends SampleData<?>, T extends ScoreStatistic<?, ?>> extends Metric<S, T>
{

    /**
     * Returns <code>true</code> if the score is decomposable in principle, false otherwise. In practice, the output 
     * may not be decomposed. For example {@link #getScoreOutputGroup()} may return {@link MetricGroup#NONE} when 
     * this method returns <code>true</code>.
     * 
     * @return true if the score is decomposable, false otherwise
     */

    boolean isDecomposable();

    /**
     * Returns <code>true</code> if the score is a relative measure or skill score, false for an absolute measure.
     * 
     * @return true if the score is a skill score
     */

    default boolean isSkillScore()
    {
        return this.getMetricName()
                   .isSkillMetric();
    }

    /**
     * Returns the group to which the score output belongs or {@link MetricGroup#NONE} if the score output does 
     * not belong to a group.
     * 
     * @return the {@link MetricGroup}
     */

    MetricGroup getScoreOutputGroup();

}

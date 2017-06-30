package wres.engine.statistics.metric;

import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.VectorOutput;

/**
 * <p>
 * The Brier Skill Score (SS) measures the reduction in the Brier Score (i.e. probabilistic Mean Square Error)
 * associated with one set of predictions when compared to another. The BSS is analogous to the
 * {@link MeanSquareErrorSkillScore} or the Nash-Sutcliffe Efficiency for a single-valued input. The perfect BSS is 1.0.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BrierSkillScore<S extends DiscreteProbabilityPairs, T extends VectorOutput>
extends
    MeanSquareErrorSkillScore<S, T>
implements ProbabilityScore
{

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class BrierSkillScoreBuilder<S extends DiscreteProbabilityPairs, T extends VectorOutput>
    extends
        MeanSquareErrorSkillScoreBuilder<S, T>
    {

        @Override
        public BrierSkillScore<S, T> build()
        {
            return new BrierSkillScore<>(this);
        }

        @Override
        public BrierSkillScoreBuilder<S, T> setDecompositionID(final MetricConstants decompositionID)
        {
            super.setDecompositionID(decompositionID);
            return this;
        }

    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BRIER_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public boolean isProper()
    {
        return false;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return false;
    }
    
    @Override
    public boolean hasRealUnits()
    {
        return false;
    }     

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private BrierSkillScore(final BrierSkillScoreBuilder<S, T> b)
    {
        super(b);
    }

}

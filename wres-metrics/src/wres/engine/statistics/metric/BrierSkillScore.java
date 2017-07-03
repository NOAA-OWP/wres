package wres.engine.statistics.metric;

import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricConstants;

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
public final class BrierSkillScore extends MeanSquareErrorSkillScore<DiscreteProbabilityPairs>
implements ProbabilityScore
{

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
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class BrierSkillScoreBuilder extends MeanSquareErrorSkillScoreBuilder<DiscreteProbabilityPairs>
    {

        @Override
        protected BrierSkillScore build()
        {
            return new BrierSkillScore(this);
        }

        @Override
        protected BrierSkillScoreBuilder setDecompositionID(final MetricConstants decompositionID)
        {
            super.setDecompositionID(decompositionID);
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private BrierSkillScore(final BrierSkillScoreBuilder b)
    {
        super(b);
    }

}

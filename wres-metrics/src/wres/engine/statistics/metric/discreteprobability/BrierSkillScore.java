package wres.engine.statistics.metric.discreteprobability;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.ProbabilityScore;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;

/**
 * <p>
 * The Brier Skill Score (SS) measures the reduction in the {@link BrierScore} (i.e. probabilistic Mean Square Error)
 * associated with one set of predictions when compared to another. The BSS is analogous to the
 * {@link MeanSquareErrorSkillScore} or the Nash-Sutcliffe Efficiency for a single-valued input. The perfect BSS is 1.0.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class BrierSkillScore extends MeanSquareErrorSkillScore<DiscreteProbabilityPairs>
        implements ProbabilityScore<DiscreteProbabilityPairs,DoubleScoreOutput>
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

    public static class BrierSkillScoreBuilder extends MeanSquareErrorSkillScoreBuilder<DiscreteProbabilityPairs>
    {

        @Override
        public BrierSkillScore build() throws MetricParameterException
        {
            return new BrierSkillScore( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private BrierSkillScore( final BrierSkillScoreBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}

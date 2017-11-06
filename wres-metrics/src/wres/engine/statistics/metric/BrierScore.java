package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;

/**
 * <p>
 * The Brier score is the average square difference between a probabilistic predictand and a verifying observation. The
 * verifying observation is also probabilistic, and is typically obtained from the indicator function of a continuous
 * variable (i.e. dichotomous). Optionally, the Brier Score may be factored into two-component or three-component
 * decompositions. By convention, the Brier Score is half of the original Brier Score proposed by Brier (1950):
 * </p>
 * <p>
 * Brier, G. W. (1950) Verification of forecasts expressed in terms of probability. <i> Monthly Weather Review</i>,
 * <b>78</b>, 1-3.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class BrierScore extends MeanSquareError<DiscreteProbabilityPairs> implements ProbabilityScore
{

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BRIER_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public boolean isProper()
    {
        return true;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class BrierScoreBuilder extends MeanSquareErrorBuilder<DiscreteProbabilityPairs>
    {

        @Override
        protected BrierScore build() throws MetricParameterException
        {
            return new BrierScore(this);
        }

        @Override
        protected BrierScoreBuilder setDecompositionID(final ScoreOutputGroup decompositionID)
        {
            super.setDecompositionID(decompositionID);
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private BrierScore(final BrierScoreBuilder builder) throws MetricParameterException
    {
        super(builder);
    }

}

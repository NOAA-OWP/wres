package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.outputs.VectorOutput;

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
public final class BrierScore<S extends DiscreteProbabilityPairs, T extends VectorOutput> extends MeanSquareError<S, T>
implements ProbabilityScore
{

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class BrierScoreBuilder<S extends DiscreteProbabilityPairs, T extends VectorOutput>
    extends
        MeanSquareErrorBuilder<S, T>
    {

        @Override
        public BrierScore<S, T> build()
        {
            return new BrierScore<>(this);
        }

        @Override
        public BrierScoreBuilder<S, T> setDecompositionID(final int decompositionID)
        {
            super.setDecompositionID(decompositionID);
            return this;
        }

    }

    @Override
    public int getID()
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
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private BrierScore(final BrierScoreBuilder<S, T> b)
    {
        super(b);
    }

}

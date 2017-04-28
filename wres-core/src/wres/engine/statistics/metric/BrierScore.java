package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

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
 */
public final class BrierScore<S extends DiscreteProbabilityPairs, T extends MetricOutput<?, ?>>
extends
    MeanSquareError<S, T>
implements ProbabilityScore
{

    @Override
    public T apply(final S s)
    {
        //TODO: implement any required decompositions, based on the instance parameters 
        return super.apply(s);
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName()
    {
        return "Brier Score";
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

    /**
     * Protected constructor.
     */

    protected BrierScore()
    {
        super();
    }

}

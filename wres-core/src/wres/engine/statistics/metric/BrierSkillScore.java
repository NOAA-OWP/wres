package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

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
public final class BrierSkillScore<S extends DiscreteProbabilityPairs, T extends MetricOutput<?, ?>>
extends
    MeanSquareErrorSkillScore<S, T>
implements ProbabilityScore
{

    @Override
    public T apply(final S s)
    {
        //TODO: implement any required decompositions, based on the instance parameters  
        return super.apply(s); //Inputs are checked in super
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName()
    {
        return "Brier Skill Score";
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

    /**
     * Protected constructor.
     */

    protected BrierSkillScore()
    {
        super();
    }

}

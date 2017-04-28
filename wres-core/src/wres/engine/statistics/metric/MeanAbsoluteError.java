package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * The mean absolute error applies to continuous variables and is the average unsigned difference between a
 * single-valued predictand and verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanAbsoluteError<S extends SingleValuedPairs, T extends ScalarOutput> extends DoubleErrorScore<S, T>
{

    @Override
    public T apply(final S s)
    {
        return super.apply(s);
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public String getName()
    {
        return "Mean Absolute Error";
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    /**
     * Protected constructor.
     */

    protected MeanAbsoluteError()
    {
        super(FunctionFactory.absError());
    }

}

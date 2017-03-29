package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * The mean error applies to continuous variables and is the average signed difference between a single-valued
 * predictand and a verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanError<S extends SingleValuedPairs, T extends ScalarOutput> extends DoubleErrorScore<S, T>
{

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function.
     */

    public static MeanError<SingleValuedPairs, ScalarOutput> newInstance()
    {
        return new MeanError();
    }

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
        return "Mean Error";
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    /**
     * Prevent direct construction.
     */

    private MeanError()
    {
        super(FunctionFactory.error());
    }

}

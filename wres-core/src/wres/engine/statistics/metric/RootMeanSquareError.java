package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * As with the MSE, the Root Mean Square Error (RMSE) or Root Mean Square Deviation (RMSD) is a measure of accuracy.
 * However, the RMSE is expressed in the original (unsquared) units of the predictand and no decompositions are
 * available for the RMSE.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class RootMeanSquareError<S extends SingleValuedPairs, T extends ScalarOutput>
extends
    MeanSquareError<S, T>
{

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function.
     */

    public static RootMeanSquareError<SingleValuedPairs, ScalarOutput> newInstance()
    {
        return new RootMeanSquareError();
    }

    @Override
    public T apply(final S t)
    {
        return (T)new ScalarOutput(Math.pow(((ScalarOutput)super.apply(t)).valueOf(), 0.5), t.size(), t.getDimension());
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName()
    {
        return "Root Mean Square Error";
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    /**
     * Prevent direct construction.
     */

    private RootMeanSquareError()
    {
        super();
    }

}

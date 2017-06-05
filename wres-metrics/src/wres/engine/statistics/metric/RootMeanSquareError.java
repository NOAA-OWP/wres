package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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
    DoubleErrorScore<S, T>
{
    /**
     * The metric name.
     */

    private static final String METRIC_NAME = "Root Mean Square Error";

    @Override
    public T apply(final S t)
    {
        return MetricOutputFactory.ofExtendsScalarOutput(Math.pow(super.apply(t).getData(), 0.5),
                                                         t.get(0).size(),
                                                         t.getDimension());
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class RootMeanSquareErrorBuilder<S extends SingleValuedPairs, T extends ScalarOutput>
    extends
        DoubleErrorScoreBuilder<S, T>
    {

        @Override
        public RootMeanSquareError<S, T> build()
        {
            return new RootMeanSquareError<>(this);
        }

    }

    @Override
    public String getName()
    {
        return METRIC_NAME;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public int getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private RootMeanSquareError(final RootMeanSquareErrorBuilder<S, T> b)
    {
        super(b.setErrorFunction(FunctionFactory.squareError()));
    }

}

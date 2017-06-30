package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanAbsoluteErrorBuilder<S extends SingleValuedPairs, T extends ScalarOutput>
    extends
        DoubleErrorScoreBuilder<S, T>
    {

        @Override
        public MeanAbsoluteError<S, T> build()
        {
            return new MeanAbsoluteError<>(this);
        }

    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public int getID()
    {
        return MetricConstants.MEAN_ABSOLUTE_ERROR;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }
    
    @Override
    public boolean hasRealUnits()
    {
        return true;
    }        

    @Override
    public int getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private MeanAbsoluteError(final MeanAbsoluteErrorBuilder<S, T> b)
    {
        super(b.setErrorFunction(FunctionFactory.absError()));
    }

}

package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanErrorBuilder<S extends SingleValuedPairs, T extends ScalarOutput>
    extends
        DoubleErrorScoreBuilder<S, T>
    {

        @Override
        public MeanError<S, T> build()
        {
            return new MeanError<>(this);
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
        return MetricConstants.MEAN_ERROR;
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
    public boolean hasRealUnits()
    {
        return true;
    }        

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private MeanError(final MeanErrorBuilder<S, T> b)
    {
        super(b.setErrorFunction(FunctionFactory.error()));
    }

}

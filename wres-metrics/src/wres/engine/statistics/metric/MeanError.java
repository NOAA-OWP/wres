package wres.engine.statistics.metric;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.SingleValuedPairs;

/**
 * The mean error applies to continuous variables and is the average signed difference between a single-valued
 * predictand and a verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanError extends DoubleErrorScore<SingleValuedPairs>
{

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_ERROR;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricConstants getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class MeanErrorBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        protected MeanError build()
        {
            return new MeanError(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private MeanError(final MeanErrorBuilder b)
    {
        super(b.setErrorFunction(FunctionFactory.error()));
    }

}

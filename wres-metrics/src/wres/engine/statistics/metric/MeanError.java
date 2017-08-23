package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;

/**
 * The mean error applies to continuous variables and is the average signed difference between a single-valued
 * predictand and a verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class MeanError extends DoubleErrorScore<SingleValuedPairs>
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
    public MetricDecompositionGroup getDecompositionID()
    {
        return MetricDecompositionGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class MeanErrorBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
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
     * @param builder the builder
     */

    private MeanError(final MeanErrorBuilder builder)
    {
        super(builder.setErrorFunction(FunctionFactory.error()));
    }

}

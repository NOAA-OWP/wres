package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.SingleValuedPairs;

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
        protected MeanError build() throws MetricParameterException
        {
            return new MeanError(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private MeanError(final MeanErrorBuilder builder) throws MetricParameterException
    {
        super(builder.setErrorFunction(FunctionFactory.error()));
    }

}

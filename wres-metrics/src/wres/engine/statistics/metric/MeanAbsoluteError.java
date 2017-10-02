package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.SingleValuedPairs;

/**
 * The mean absolute error applies to continuous variables and is the average unsigned difference between a
 * single-valued predictand and verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class MeanAbsoluteError extends DoubleErrorScore<SingleValuedPairs>
{

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_ABSOLUTE_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class MeanAbsoluteErrorBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        protected MeanAbsoluteError build()
        {
            return new MeanAbsoluteError(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private MeanAbsoluteError(final MeanAbsoluteErrorBuilder builder)
    {
        super(builder.setErrorFunction(FunctionFactory.absError()));
    }

}

package wres.engine.statistics.metric.singlevalued;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The mean absolute error applies to continuous variables and is the average unsigned difference between a
 * single-valued predictand and verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanAbsoluteError extends DoubleErrorScore<SingleValuedPairs>
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

    public static class MeanAbsoluteErrorBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        public MeanAbsoluteError build() throws MetricParameterException
        {
            return new MeanAbsoluteError(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private MeanAbsoluteError(final MeanAbsoluteErrorBuilder builder) throws MetricParameterException
    {
        super(builder.setErrorFunction(FunctionFactory.absError()));
    }

}

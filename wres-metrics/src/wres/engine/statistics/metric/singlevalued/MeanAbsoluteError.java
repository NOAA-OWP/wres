package wres.engine.statistics.metric.singlevalued;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The mean absolute error applies to continuous variables and is the average unsigned difference between a
 * single-valued predictand and verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanAbsoluteError extends DoubleErrorScore<SingleValuedPairs>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static MeanAbsoluteError of()
    {
        return new MeanAbsoluteError();
    }
    
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
     * Hidden constructor.
     */

    private MeanAbsoluteError()
    {
        super( FunctionFactory.absError() );
    }

}

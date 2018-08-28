package wres.engine.statistics.metric.singlevalued;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The mean error applies to continuous variables and is the average signed difference between a single-valued
 * predictand and a verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanError extends DoubleErrorScore<SingleValuedPairs>
{
    
    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static MeanError of()
    {
        return new MeanError();
    }

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
     * Hidden constructor.
     */

    private MeanError()
    {
        super( FunctionFactory.error() );
    }

}

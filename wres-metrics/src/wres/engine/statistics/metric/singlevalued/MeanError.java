package wres.engine.statistics.metric.singlevalued;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;

/**
 * The mean error applies to continuous variables and is the average signed difference between a single-valued
 * predictand and a verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanError extends DoubleErrorScore<SampleData<Pair<Double,Double>>>
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
        super();
    }

}

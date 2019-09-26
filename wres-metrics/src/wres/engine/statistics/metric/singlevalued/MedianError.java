package wres.engine.statistics.metric.singlevalued;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The median error applies to continuous variables and is the median signed difference 
 * between a single-valued predictand and a verifying observation. It measures the 
 * median bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MedianError extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MedianError of()
    {
        return new MedianError();
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEDIAN_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    private MedianError()
    {
        super( FunctionFactory.error(), FunctionFactory.median() );
    }

}

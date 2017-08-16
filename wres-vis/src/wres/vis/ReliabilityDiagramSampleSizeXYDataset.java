package wres.vis;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;

/**
 * The {@link XYDataset} for use in building the sample size portion of the reliability diagram plot (the other being
 * the reliability diagram portion).
 * 
 * @author Hank.Herr
 */
public class ReliabilityDiagramSampleSizeXYDataset extends WRESAbstractXYDataset<MetricOutputMapByLeadThreshold<MultiVectorOutput>,MetricOutputMapByLeadThreshold<MultiVectorOutput>>
{
    
    public ReliabilityDiagramSampleSizeXYDataset(final MetricOutputMapByLeadThreshold<MultiVectorOutput> input)
    {
        super(input);
    }

    @Override
    protected void preparePlotData(final MetricOutputMapByLeadThreshold<MultiVectorOutput> rawData)
    {
        //This check should not be necessary, since the conditions should be impossible.  I'll do it anyway just to be sure.
        if((rawData.keySetByFirstKey().size() == 0) || (rawData.keySetBySecondKey().size() == 0))
        {
            throw new IllegalStateException("Somehow, one of the key sets, either first or second, is empty.  How did that happen?");
        }
        
        setPlotData(rawData);
    }
    
    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().get(getPlotData().getKey(series)).getData().get(MetricConstants.FORECAST_PROBABILITY).getDoubles().length;
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return getPlotData().get(getPlotData().getKey(series)).getData().get(MetricConstants.FORECAST_PROBABILITY).getDoubles()[item];
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().get(getPlotData().getKey(series)).getData().get(MetricConstants.SAMPLE_SIZE).getDoubles()[item];
    }

    @Override
    public int getSeriesCount()
    {
        return getPlotData().keySet().size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        if (isLegendNameOverridden(series))
        {
            return getOverrideLegendName(series);
        }
        
        if((getPlotData().keySetByFirstKey().size() == 1) && (getPlotData().keySetBySecondKey().size() == 1))
        {
            return getPlotData().getKey(series).getFirstKey().toString() + "h, " + getPlotData().getKey(series).getSecondKey().toString();
        }
        else if((getPlotData().keySetByFirstKey().size() >= 1) && (getPlotData().keySetBySecondKey().size() == 1))
        {
            return getPlotData().getKey(series).getFirstKey().toString();
        } 
        else
        {
            return getPlotData().getKey(series).getSecondKey().toString();
        } 
    }

}

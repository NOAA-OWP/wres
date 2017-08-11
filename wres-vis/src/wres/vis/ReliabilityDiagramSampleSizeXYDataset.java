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
public class ReliabilityDiagramSampleSizeXYDataset extends WRESAbstractXYDataset
{
    public ReliabilityDiagramSampleSizeXYDataset(final MetricOutputMapByLeadThreshold<MultiVectorOutput> input)
    {
        super(input);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void preparePlotData(final Object rawData)
    {
        final MetricOutputMapByLeadThreshold<MultiVectorOutput> input =
                                                                      (MetricOutputMapByLeadThreshold<MultiVectorOutput>)rawData;
        if(input.keySetByFirstKey().size() != 1)
        {
            throw new IllegalArgumentException("MetricOutputMapByLeadThreshold map provided has "
                + input.keySetByFirstKey().size() + " keys, when it must be one key, only.");
        }
        setPlotData(input);
    }

    @SuppressWarnings("unchecked")
    @Override
    public MetricOutputMapByLeadThreshold<MultiVectorOutput> getPlotData()
    {
        return (MetricOutputMapByLeadThreshold<MultiVectorOutput>)getPlotDataAsObject();
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
        return getPlotData().getKey(series).getSecondKey().toString();
    }

}

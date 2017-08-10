package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.AbstractXYDataset;
import org.jfree.data.xy.XYDataset;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;

/**
 * The {@link XYDataset} for use in building the reliability diagram portion of the reliability diagram plot (the other
 * being the sample size portion).
 * 
 * @author Hank.Herr
 */
public class ReliabilityDiagramXYDataset extends AbstractXYDataset
{
    private final transient MetricOutputMapByLeadThreshold<MultiVectorOutput> data;

    private final List<String> legendNames = new ArrayList<>();
    
    public ReliabilityDiagramXYDataset(final MetricOutputMapByLeadThreshold<MultiVectorOutput> input)
    {
        if (input.keySetByFirstKey().size() != 1)
        {
            System.err.println("####>> " + input.keySetByFirstKey().size());
            throw new IllegalArgumentException("MetricOutputMapByLeadThreshold map provided has more than one key, which is not allowed.");
        }
        data = input;
        
        //Populate the legend names.
        for (int i = 0; i < getSeriesCount(); i ++)
        {
            legendNames.add(null);
        }
    }

    public void setLegendName(final int index, final String name)
    {
        legendNames.set(index, name);
    }
    
    @Override
    public int getItemCount(final int series)
    {
        return data.get(data.getKey(series)).getData().get(MetricConstants.FORECAST_PROBABILITY).getDoubles().length;
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return data.get(data.getKey(series)).getData().get(MetricConstants.FORECAST_PROBABILITY).getDoubles()[item];
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return data.get(data.getKey(series)).getData().get(MetricConstants.OBSERVED_GIVEN_FORECAST_PROBABILITY).getDoubles()[item];
    }

    @Override
    public int getSeriesCount()
    {
        return data.keySet().size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        if (legendNames.get(series) != null)
        {
            return legendNames.get(series);
        }
        return data.getKey(series).getSecondKey().toString();
    }

}

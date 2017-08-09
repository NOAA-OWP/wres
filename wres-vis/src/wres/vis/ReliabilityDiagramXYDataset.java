package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;

public class ReliabilityDiagramXYDataset extends AbstractXYDataset
{


    /**
     * Legend items.
     */

    private final List<String> legendNames = new ArrayList<>();


    /**
     * Data sliced by series, i.e. one threshold per slice, where each slice contains all lead times for one score.
     */

    private final transient MetricOutputMapByLeadThreshold<MultiVectorOutput> data;

    /**
     * 
     * @param input The input must already be sliced for a single lead time!
     */
    public ReliabilityDiagramXYDataset(final MetricOutputMapByLeadThreshold<MultiVectorOutput> input)
    {
        //Slice the input data by threshold and store locally.  The result of this is that each item in the 
        //data list has a single key entry, so that the value of that kay provides the series values to plot.
        data = input;
        input.keySetByThreshold().forEach(key -> {
            legendNames.add(key.toString());
        });
    }

    /**
     * Set the legend name.
     * 
     * @param series the series index
     * @param name the name
     */
    
    public void setLegendName(final int series, final String name)
    {
        legendNames.set(series, name);
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
        return legendNames.size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        return legendNames.get(series);
    }

}

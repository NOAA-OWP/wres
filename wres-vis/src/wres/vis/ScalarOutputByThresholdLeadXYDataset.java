package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.ScalarOutput;

/**
 * An {@link AbstractXYDataset} that wraps a {@link MetricOutputMapByLeadThreshold} which contains a set of
 * {@link ScalarOutput} for a single verification metric, indexed by forecast lead time and threshold. Slices the data
 * by lead time to form plots by threshold on the domain axis.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ScalarOutputByThresholdLeadXYDataset extends WRESAbstractXYDataset
{
    private static final long serialVersionUID = 1598160458133121056L;

    public ScalarOutputByThresholdLeadXYDataset(final MetricOutputMapByLeadThreshold<ScalarOutput> input)
    {
        super(input);

        //Handling the legend name in here because otherwise the key will be lost (I don't keep the raw data).
        int seriesIndex = 0;
        for(final Integer lead: input.keySetByLead())
        {
            setOverrideLegendName(seriesIndex, lead.toString() + "h");
            seriesIndex++;
        }
    }

    /**
     * The legend names are handled here with calls to {@link #setOverrideLegendName(int, String)} because the first
     * keys (the thresholds) will otherwise be lost when the data is populated.
     * 
     * @param rawData the input data must be of type {@link MetricOutputMapByLeadThreshold} with generic
     *            {@link ScalarOutput}.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void preparePlotData(final Object rawData)
    {
        //Cast the raw data input and check the size.
        final MetricOutputMapByLeadThreshold<ScalarOutput> input =
                                                                 (MetricOutputMapByLeadThreshold<ScalarOutput>)rawData;
        final List<MetricOutputMapByLeadThreshold<ScalarOutput>> data = new ArrayList<>();
        for(final Integer lead: input.keySetByLead())
        {
            data.add(input.sliceByLead(lead));
        }
        setPlotData(data);
    }

    /**
     * Data sliced by series, i.e. one lead time per slice, where each slice contains all thresholds for one score.
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<MetricOutputMapByLeadThreshold<ScalarOutput>> getPlotData()
    {
        return (List<MetricOutputMapByLeadThreshold<ScalarOutput>>)getPlotDataAsObject();
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().get(series).size();
    }

    @Override
    public Number getX(final int series, final int item)
    {
        //Cannot allow all data (infinite) threshold. Use lower bound if this is a "BETWEEN" threshold
        final double test = getPlotData().get(series).getKey(item).getSecondKey().getThreshold();
        if(Double.isInfinite(test))
        {
            return Double.MIN_VALUE; //JFreeChart missing protocol is to return finite double for X and null for Y
        }
        return test;
    }

    @Override
    public Number getY(final int series, final int item)
    {
        //Cannot allow all data (infinite) threshold
        final Double test = (Double)getX(series, item);
        if(test.equals(Double.MIN_VALUE))
        {
            return null;
        }
        return getPlotData().get(series).getValue(item).getData();
    }

    @Override
    public int getSeriesCount()
    {
        return getPlotData().size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        return getOverrideLegendName(series);
    }
}

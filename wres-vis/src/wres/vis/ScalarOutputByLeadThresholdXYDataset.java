package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.Threshold;

/**
 * An {@link AbstractXYDataset} that wraps a {@link MetricOutputMapByLeadThreshold} which contains a set of
 * {@link ScalarOutput} for a single verification metric, indexed by forecast lead time and threshold. Slices the data
 * by threshold to form plots by lead time on the domain axis.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ScalarOutputByLeadThresholdXYDataset extends WRESAbstractXYDataset
{
    private static final long serialVersionUID = 2251263309545763140L;

    public ScalarOutputByLeadThresholdXYDataset(final MetricOutputMapByLeadThreshold<ScalarOutput> input)
    {
        super(input);

        //Handling the legend name in here because otherwise the key will be lost (I don't keep the raw data).
        //The data is processed into a list based on the key that must appear in the legend.
        int seriesIndex = 0;
        for(final Threshold key: input.keySetByThreshold())
        {
            setOverrideLegendName(seriesIndex, key.toString());
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
        final MetricOutputMapByLeadThreshold<ScalarOutput> input =
                                                                 (MetricOutputMapByLeadThreshold<ScalarOutput>)rawData;
        final List<MetricOutputMapByLeadThreshold<ScalarOutput>> data = new ArrayList<>();
        for(final Threshold key: input.keySetByThreshold())
        {
            data.add(input.sliceByThreshold(key));
        }
        setPlotData(data);
    }

    /**
     * @return Data sliced by series, i.e. one threshold per slice, where each slice contains all lead times for one
     *         score.
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
        return getPlotData().get(series).getKey(item).getFirstKey();
    }

    @Override
    public Number getY(final int series, final int item)
    {
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

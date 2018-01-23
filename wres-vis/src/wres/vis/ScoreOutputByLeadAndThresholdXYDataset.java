package wres.vis;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.Threshold;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;

/**
 * An {@link AbstractXYDataset} that wraps a {@link MetricOutputMapByTimeAndThreshold} which contains a set of
 * {@link ScoreOutput} for a single verification metric, indexed by forecast lead time and threshold. Slices the data
 * by threshold to form plots by lead time on the domain axis.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ScoreOutputByLeadAndThresholdXYDataset extends
        WRESAbstractXYDataset<List<MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>>
{
    private static final long serialVersionUID = 2251263309545763140L;

    public ScoreOutputByLeadAndThresholdXYDataset(final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input)
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
     * @param rawData the input data must be of type {@link MetricOutputMapByTimeAndThreshold} with generic
     *            {@link ScoreOutput}.
     */
    @Override
    protected void preparePlotData(final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rawData)
    {
        final List<MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> data = new ArrayList<>();
        for(final Threshold key: rawData.keySetByThreshold())
        {
            data.add(rawData.filterByThreshold(key));
        }
        setPlotData(data);
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().get(series).size();
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return getPlotData().get(series).getKey(item).getLeft().getLatestLeadTimeInHours();
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

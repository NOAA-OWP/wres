package wres.vis;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.Slicer;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * An {@link AbstractXYDataset} that wraps a {@link ListOfStatistics} which contains a set of
 * {@link ScoreStatistic} for a single verification metric, indexed by forecast lead time and threshold. Slices the data
 * by threshold to form plots by lead time on the domain axis.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ScoreOutputByLeadAndThresholdXYDataset extends
        WRESAbstractXYDataset<List<ListOfStatistics<DoubleScoreStatistic>>, ListOfStatistics<DoubleScoreStatistic>>
{
    private static final long serialVersionUID = 2251263309545763140L;

    public ScoreOutputByLeadAndThresholdXYDataset(final ListOfStatistics<DoubleScoreStatistic> input)
    {
        super(input);

        //Handling the legend name in here because otherwise the key will be lost (I don't keep the raw data).
        //The data is processed into a list based on the key that must appear in the legend.
        int seriesIndex = 0;
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( input, next -> next.getMetadata().getSampleMetadata().getThresholds() );
        for ( final OneOrTwoThresholds key : thresholds )
        {
            setOverrideLegendName( seriesIndex, key.toStringWithoutUnits() );
            seriesIndex++;
        }
    }

    /**
     * The legend names are handled here with calls to {@link #setOverrideLegendName(int, String)} because the first
     * keys (the thresholds) will otherwise be lost when the data is populated.
     * 
     * @param rawData the input data must be of type {@link ListOfStatistics} with generic
     *            {@link ScoreStatistic}.
     */
    @Override
    protected void preparePlotData( final ListOfStatistics<DoubleScoreStatistic> rawData )
    {
        final List<ListOfStatistics<DoubleScoreStatistic>> data = new ArrayList<>();
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( rawData, next -> next.getMetadata().getSampleMetadata().getThresholds() );
        for ( final OneOrTwoThresholds key : thresholds )
        {
            data.add( Slicer.filter( rawData, next -> next.getSampleMetadata().getThresholds().equals( key ) ) );
        }
        setPlotData( data );
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().get(series).getData().size();
    }

    @Override
    public Number getX( final int series, final int item )
    {
        return getPlotData().get( series )
                            .getData()
                            .get( item )
                            .getMetadata()
                            .getSampleMetadata()
                            .getTimeWindow()
                            .getLatestLeadTime().toHours();
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().get(series).getData().get(item).getData();
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

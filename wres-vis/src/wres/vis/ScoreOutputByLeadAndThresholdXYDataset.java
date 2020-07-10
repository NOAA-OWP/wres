package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.Slicer;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.util.TimeHelper;

/**
 * An {@link AbstractXYDataset} that wraps a {@link List} which contains a set of
 * {@link ScoreStatistic} for a single verification metric, indexed by forecast lead time and threshold. Slices the data
 * by threshold to form plots by lead time on the domain axis.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ScoreOutputByLeadAndThresholdXYDataset extends
        WRESAbstractXYDataset<List<List<DoubleScoreComponentOuter>>, List<DoubleScoreComponentOuter>>
{
    private static final long serialVersionUID = 2251263309545763140L;

    /**
     * The duration units.
     */
    
    private final ChronoUnit durationUnits;
    
    /**
     * Build a new score output by lead duration and threshold.
     * 
     * @param input the list of inputs to plot
     * @param durationUnits the duration units
     * @throws NullPointerException if any input is null
     */

    public ScoreOutputByLeadAndThresholdXYDataset( final List<DoubleScoreComponentOuter> input,
                                                   final ChronoUnit durationUnits )
    {
        super( input );

        Objects.requireNonNull( input, "Specify non-null input." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        this.durationUnits = durationUnits;

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
     * @param rawData the input data must be of type {@link List} with generic
     *            {@link DoubleScoreComponentOuter}.
     */
    @Override
    protected void preparePlotData( final List<DoubleScoreComponentOuter> rawData )
    {
        final List<List<DoubleScoreComponentOuter>> data = new ArrayList<>();
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
        return getPlotData().get(series).size();
    }

    @Override
    public Number getX( final int series, final int item )
    {
        return TimeHelper.durationToLongUnits( this.getPlotData()
                                                   .get( series )
                                                   .get( item )
                                                   .getMetadata()
                                                   .getSampleMetadata()
                                                   .getTimeWindow()
                                                   .getLatestLeadDuration(),
                                               this.durationUnits );
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().get(series).get(item).getData().getValue();
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

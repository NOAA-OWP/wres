package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.Slicer;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.time.TimeWindow;
import wres.util.TimeHelper;

/**
 * An {@link AbstractXYDataset} that wraps a {@link List} which contains a set of
 * {@link ScoreStatistic} for a single verification metric, indexed by forecast lead time and threshold. Slices the data
 * by lead time to form plots by threshold on the domain axis.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ScoreOutputByThresholdAndLeadXYDataset extends
        WRESAbstractXYDataset<List<List<DoubleScoreStatistic>>, List<DoubleScoreStatistic>>
{
    private static final long serialVersionUID = 1598160458133121056L;

    /**
     * The duration units.
     */
    
    private final ChronoUnit durationUnits;
    
    /**
     * Build a new score output by threshold and lead duration.
     * 
     * @param input the list of inputs to plot
     * @param durationUnits the duration units
     * @throws NullPointerException if any input is null
     */

    public ScoreOutputByThresholdAndLeadXYDataset( final List<DoubleScoreStatistic> input,
                                                   final ChronoUnit durationUnits )
    {
        super( input );

        Objects.requireNonNull( input, "Specify non-null input." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        this.durationUnits = durationUnits;
        
        //Handling the legend name in here because otherwise the key will be lost (I don't keep the raw data).
        int seriesIndex = 0;
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( input, next -> next.getMetadata().getSampleMetadata().getTimeWindow() );
        for ( final TimeWindow lead : timeWindows )
        {
            setOverrideLegendName( seriesIndex,
                                   Long.toString( TimeHelper.durationToLongUnits( lead.getLatestLeadDuration(),
                                                                                  this.durationUnits ) ) );
            seriesIndex++;
        }
    }

    /**
     * The legend names are handled here with calls to {@link #setOverrideLegendName(int, String)} because the first
     * keys (the thresholds) will otherwise be lost when the data is populated.
     * 
     * @param rawData the input data must be of type {@link List} with generic
     *            {@link DoubleScoreStatistic}.
     */
    @Override
    protected void preparePlotData( final List<DoubleScoreStatistic> rawData )
    {
        //Cast the raw data input and check the size.
        final List<List<DoubleScoreStatistic>> data = new ArrayList<>();
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( rawData, next -> next.getMetadata().getSampleMetadata().getTimeWindow() );
        for ( final TimeWindow lead : timeWindows )
        {
            data.add( Slicer.filter( rawData, next -> next.getSampleMetadata().getTimeWindow().equals( lead ) ) );
        }
        setPlotData( data );
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
        final double test = getPlotData().get( series )
                                         .get( item )
                                         .getMetadata()
                                         .getSampleMetadata()
                                         .getThresholds()
                                         .first()
                                         .getValues()
                                         .first();
        if ( Double.isInfinite( test ) )
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
        return getPlotData().get(series).get(item).getData();
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

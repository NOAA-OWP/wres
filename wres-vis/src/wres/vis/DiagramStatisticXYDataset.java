package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.util.TimeHelper;

/**
 * The {@link XYDataset} for use in building a chart that plots a {@link DiagramStatisticOuter}.
 * 
 * @author Hank.Herr
 */
public class DiagramStatisticXYDataset
        extends WRESAbstractXYDataset<List<DiagramStatisticOuter>, List<DiagramStatisticOuter>>
{
    private static final long serialVersionUID = 4254109136599641286L;
    private final MetricDimension xConstant;
    private final MetricDimension yConstant;

    /**
     * The duration units.
     */
    
    private final ChronoUnit durationUnits;
    
    /**
     * Build a new diagram.
     * 
     * @param input the list of inputs to plot
     * @param xConstant the dimension for the domain axis
     * @param yConstant the dimension for the range axis
     * @param durationUnits the duration units
     * @throws NullPointerException if any input is null
     */

    public DiagramStatisticXYDataset( final List<DiagramStatisticOuter> input,
                                              final MetricDimension xConstant,
                                              final MetricDimension yConstant,
                                              final ChronoUnit durationUnits )
    {
        super( input );
        
        Objects.requireNonNull( input, "Specify non-null input." );
        
        Objects.requireNonNull( xConstant, "Specify a non-null domain axis dimension." );
        
        Objects.requireNonNull( yConstant, "Specify a non-null range axis dimension." );
        
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        
        this.xConstant = xConstant;
        this.yConstant = yConstant;
        this.durationUnits = durationUnits;
    }

    @Override
    void preparePlotData(final List<DiagramStatisticOuter> rawData)
    {
        //This check should not be necessary, since the conditions should be impossible.  I'll do it anyway just to be sure.
        if ( rawData.isEmpty() )
        {
            throw new IllegalArgumentException( "Specify non-empty input." );
        }

        setPlotData(rawData);
    }

    @Override
    public int getItemCount(final int series)
    {
        return getPlotData().get(series)
                            .getData()
                            .get(xConstant)
                            .getDoubles().length;
    }

    @Override
    public Number getX(final int series, final int item)
    {
        return getPlotData().get(series)
                            .getData()
                            .get(xConstant)
                            .getDoubles()[item];
    }

    @Override
    public Number getY(final int series, final int item)
    {
        return getPlotData().get(series)
                            .getData()
                            .get(yConstant)
                            .getDoubles()[item];
    }

    @Override
    public int getSeriesCount()
    {
        return getPlotData().size();
    }

    @Override
    public Comparable<String> getSeriesKey(final int series)
    {
        if (isLegendNameOverridden(series))
        {
            return getOverrideLegendName(series);
        }
        
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( getPlotData(), meta -> meta.getMetadata().getSampleMetadata().getTimeWindow() );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( getPlotData(), meta -> meta.getMetadata().getSampleMetadata().getThresholds() );

        if ( ( timeWindows.size() == 1 ) && ( thresholds.size() == 1 ) )
        {
            return "All Data"; //All of the data is in a single series.
        }
        else if ( ( !timeWindows.isEmpty() ) && ( thresholds.size() == 1 ) )
        {
            return Long.toString( TimeHelper.durationToLongUnits( getPlotData().get( series )
                                                                               .getMetadata()
                                                                               .getSampleMetadata()
                                                                               .getTimeWindow()
                                                                               .getLatestLeadDuration(),
                                                                  this.durationUnits ) );
        }
        else
        {
            return getPlotData().get( series )
                                .getMetadata()
                                .getSampleMetadata()
                                .getThresholds()
                                .toStringWithoutUnits();
        }
    }

}

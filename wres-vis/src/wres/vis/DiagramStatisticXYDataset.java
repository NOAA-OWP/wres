package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants.MetricDimension;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.util.TimeHelper;

/**
 * The {@link XYDataset} for use in building a chart that plots a {@link DiagramStatisticOuter}.
 * 
 * @author Hank.Herr
 */
class DiagramStatisticXYDataset
        extends WRESAbstractXYDataset<List<DiagramStatisticOuter>, List<DiagramStatisticOuter>>
{
    private static final long serialVersionUID = 4741283362664043745L;
    private final DiagramComponentName xConstant;
    private final DiagramComponentName yConstant;

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

    DiagramStatisticXYDataset( final List<DiagramStatisticOuter> input,
                               final MetricDimension xConstant,
                               final MetricDimension yConstant,
                               final ChronoUnit durationUnits )
    {
        super( input );

        Objects.requireNonNull( input, "Specify non-null input." );

        Objects.requireNonNull( xConstant, "Specify a non-null domain axis dimension." );

        Objects.requireNonNull( yConstant, "Specify a non-null range axis dimension." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        this.xConstant = DiagramComponentName.valueOf( xConstant.name() );
        this.yConstant = DiagramComponentName.valueOf( yConstant.name() );
        this.durationUnits = durationUnits;
    }

    @Override
    void preparePlotData( final List<DiagramStatisticOuter> rawData )
    {
        //This check should not be necessary, since the conditions should be impossible.  I'll do it anyway just to be sure.
        if ( rawData.isEmpty() )
        {
            throw new IllegalArgumentException( "Specify non-empty input." );
        }
        
        super.setPlotData( rawData );
    }

    @Override
    public int getItemCount( final int series )
    {
        Optional<DiagramStatisticComponent> number = getPlotData().get( series )
                                                                  .getData()
                                                                  .getStatisticsList()
                                                                  .stream()
                                                                  .filter( next -> this.xConstant == next.getMetric()
                                                                                                         .getName() )
                                                                  .findFirst();

        if ( number.isPresent() )
        {
            return number.get().getValuesCount();
        }

        return 0;
    }

    @Override
    public Number getX( final int series, final int item )
    {
        Optional<DiagramStatisticComponent> number = getPlotData().get( series )
                                                                  .getData()
                                                                  .getStatisticsList()
                                                                  .stream()
                                                                  .filter( next -> this.xConstant == next.getMetric()
                                                                                                         .getName() )
                                                                  .findFirst();

        if ( number.isPresent() )
        {
            return number.get().getValues( item );
        }

        return Double.NaN;
    }

    @Override
    public Number getY( final int series, final int item )
    {
        Optional<DiagramStatisticComponent> number = getPlotData().get( series )
                                                                  .getData()
                                                                  .getStatisticsList()
                                                                  .stream()
                                                                  .filter( next -> this.yConstant == next.getMetric()
                                                                                                         .getName() )
                                                                  .findFirst();

        if ( number.isPresent() )
        {
            return number.get().getValues( item );
        }

        return Double.NaN;
    }

    @Override
    public int getSeriesCount()
    {
        return this.getPlotData()
                   .size();
    }

    @Override
    public Comparable<String> getSeriesKey( final int series )
    {
        if ( isLegendNameOverridden( series ) )
        {
            return getOverrideLegendName( series );
        }

        DiagramStatisticOuter diagram = this.getPlotData()
                                            .get( series );

        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( this.getPlotData(), meta -> meta.getMetadata().getTimeWindow() );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( this.getPlotData(), meta -> meta.getMetadata().getThresholds() );

        // Qualifier for dimensions that are repeated, such as quantile curves in an ensemble QQ diagram
        String qualifier = diagram.getData()
                                  .getStatistics( 0 )
                                  .getName();

        // One time window and one or more thresholds: label by threshold
        if ( timeWindows.size() == 1 )
        {
            // If there is a qualifier, then there is a single threshold and up to N named components, else up to M
            // thresholds and one named component
            if( !qualifier.isBlank() )
            {
                return qualifier;
            }
            
            return diagram.getMetadata()
                          .getThresholds()
                          .toStringWithoutUnits();
        }
        // One threshold and one or more time windows: label by time window
        else if ( thresholds.size() == 1 )
        {
            return Long.toString( TimeHelper.durationToLongUnits( diagram.getMetadata()
                                                                         .getTimeWindow()
                                                                         .getLatestLeadDuration(),
                                                                  this.durationUnits ) )
                   + ", " + qualifier;
        }
        else
        {
            throw new IllegalStateException( "Unexpected data configuration for the diagram." );
        }
    }

}

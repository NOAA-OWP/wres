package wres.vis.data;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.AbstractXYDataset;
import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants.MetricDimension;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.vis.GraphicsUtils;

/**
 * Creates an {@link XYDataset} for plotting a verification diagram.
 * 
 * In order to support displays of intervals, such as confidence intervals, upgrade to 
 * {@link AbstractIntervalXYDataset}.
 * 
 * @author James Brown
 */

class Diagram extends AbstractXYDataset
{

    /** Serial version identifier. */
    private static final long serialVersionUID = -6260904713900350909L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Diagram.class );

    /** The statistics to plot, arranged by series. Each item in the outer list contains a series, indexed by name. 
     *  The first */
    private final List<Pair<String, Pair<DiagramStatisticComponent, DiagramStatisticComponent>>> statistics;

    /**
     * @param statistics the statistics
     * @param xDimension the dimension that corresponds to the x axis in the plot
     * @param yDimension the dimension that corresponds to the y axis in the plot
     * @param durationUnits the lead duration units
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    static Diagram of( List<DiagramStatisticOuter> statistics,
                              MetricDimension xDimension,
                              MetricDimension yDimension,
                              ChronoUnit durationUnits )
    {
        return new Diagram( statistics, xDimension, yDimension, durationUnits );
    }

    @Override
    public int getItemCount( int series )
    {
        return this.statistics.get( series )
                              .getRight()
                              .getLeft()
                              .getValuesCount();
    }

    @Override
    public Number getX( int series, int item )
    {
        return this.statistics.get( series )
                              .getRight()
                              .getLeft()
                              .getValues( item );
    }

    @Override
    public Number getY( int series, int item )
    {
        return this.statistics.get( series )
                              .getRight()
                              .getRight()
                              .getValues( item );
    }

    @Override
    public int getSeriesCount()
    {
        return this.statistics.size();
    }

    @Override
    public Comparable<String> getSeriesKey( int series )
    {
        return this.statistics.get( series )
                              .getLeft();
    }

    /**
     * Hidden constructor.
     * @param statistics the statistics
     * @param xDimension the dimension that corresponds to the x axis in the plot
     * @param yDimension the dimension that corresponds to the y axis in the plot 
     * @param durationUnits the lead duration units
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the data has an unexpected shape
     */

    private Diagram( List<DiagramStatisticOuter> statistics,
                     MetricDimension xDimension,
                     MetricDimension yDimension,
                     ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( xDimension );
        Objects.requireNonNull( yDimension );
        Objects.requireNonNull( durationUnits );

        int timeWindowCount = Slicer.discover( statistics, meta -> meta.getMetadata().getTimeWindow() )
                                    .size();

        int thresholdCount = Slicer.discover( statistics, meta -> meta.getMetadata().getThresholds() )
                                   .size();

        // Create the series
        List<Pair<String, Pair<DiagramStatisticComponent, DiagramStatisticComponent>>> innerStatistics =
                new ArrayList<>();

        for ( DiagramStatisticOuter nextSeries : statistics )
        {
            String seriesName = Diagram.getSeriesName( nextSeries, timeWindowCount, thresholdCount, durationUnits );
            String nameQualifier = nextSeries.getComponentNameQualifiers()
                                             .first();

            DiagramStatisticComponent xData = nextSeries.getComponent( xDimension, nameQualifier );
            DiagramStatisticComponent yData = nextSeries.getComponent( yDimension, nameQualifier );

            if ( Objects.isNull( xData ) || Objects.isNull( yData ) )
            {
                throw new IllegalArgumentException( "Could not discover the expected diagram components for "
                                                    + "series "
                                                    + seriesName
                                                    + ". The expected x-axis dimension was "
                                                    + xDimension
                                                    + " and the expected y-axis dimension was "
                                                    + yDimension
                                                    + "." );
            }

            Pair<DiagramStatisticComponent, DiagramStatisticComponent> series = Pair.of( xData, yData );
            Pair<String, Pair<DiagramStatisticComponent, DiagramStatisticComponent>> namedSeries =
                    Pair.of( seriesName, series );

            innerStatistics.add( namedSeries );
        }

        this.statistics = Collections.unmodifiableList( innerStatistics );

        LOGGER.debug( "Created a Diagram dataset with {} series.", this.statistics.size() );
    }


    /**
     * @param diagram the diagram series whose name is required
     * @param timeWindowCount the number of time windows across all series
     * @param thresholdCount the number of thresholds across all series
     * @param durationUnits the duration units
     * @return the series name
     * @throws IllegalArgumentException if the counts are not supported
     */

    private static String getSeriesName( DiagramStatisticOuter diagram,
                                         int timeWindowCount,
                                         int thresholdCount,
                                         ChronoUnit durationUnits )
    {
        // Qualifier for dimensions that are repeated, such as quantile curves in an ensemble QQ diagram
        String qualifier = diagram.getData()
                                  .getStatistics( 0 )
                                  .getName();

        // One time window and one or more thresholds: label by threshold
        if ( timeWindowCount == 1 )
        {
            // If there is a qualifier, then there is a single threshold and up to N named components, else up to M
            // thresholds and one named component
            if ( !qualifier.isBlank() )
            {
                return qualifier;
            }

            return diagram.getMetadata()
                          .getThresholds()
                          .toStringWithoutUnits();
        }
        // One threshold and one or more time windows: label by time window
        else if ( thresholdCount == 1 )
        {
            return Long.toString( GraphicsUtils.durationToLongUnits( diagram.getMetadata()
                                                                            .getTimeWindow()
                                                                            .getLatestLeadDuration(),
                                                                     durationUnits ) )
                   + ", "
                   + qualifier;
        }
        else
        {
            throw new IllegalArgumentException( "Unexpected data configuration for the diagram. Expected a single time "
                                                + "window and one or more thresholds or a single threshold and one or "
                                                + "more time windows, but found "
                                                + timeWindowCount
                                                + " time windows and "
                                                + thresholdCount
                                                + " thresholds." );
        }
    }

}

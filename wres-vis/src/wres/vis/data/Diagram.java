package wres.vis.data;

import java.io.Serial;
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

import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * <p>Creates an {@link XYDataset} for plotting a verification diagram.
 * 
 * <p>In order to support displays of intervals, such as confidence intervals, upgrade to
 * {@link AbstractIntervalXYDataset}.
 * 
 * @author James Brown
 */

class Diagram extends AbstractXYDataset
{

    /** Serial version identifier. */
    @Serial
    private static final long serialVersionUID = -6260904713900350909L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Diagram.class );

    /** The statistics to plot, arranged by series. Each item in the outer list contains a series, indexed by name. 
     *  The first */
    private final List<Pair<String, Pair<DiagramStatisticComponent, DiagramStatisticComponent>>> statistics;

    /**
     * Returns a dataset with one series per threshold for a single lead duration
     * @param statistics the statistics
     * @param xDimension the dimension that corresponds to the x axis in the plot
     * @param yDimension the dimension that corresponds to the y axis in the plot
     * @param durationUnits the lead duration units
     * @return an instance
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the dataset contains more than one lead duration
     */

    static Diagram ofLeadThreshold( List<DiagramStatisticOuter> statistics,
                                    MetricDimension xDimension,
                                    MetricDimension yDimension,
                                    ChronoUnit durationUnits )
    {
        return new Diagram( statistics, xDimension, yDimension, durationUnits, true );
    }

    /**
     * Returns a dataset with one series per lead duration for a single threshold
     * @param statistics the statistics
     * @param xDimension the dimension that corresponds to the x axis in the plot
     * @param yDimension the dimension that corresponds to the y axis in the plot
     * @param durationUnits the lead duration units
     * @return an instance
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the dataset contains more than one threshold
     */

    static Diagram ofThresholdLead( List<DiagramStatisticOuter> statistics,
                                    MetricDimension xDimension,
                                    MetricDimension yDimension,
                                    ChronoUnit durationUnits )
    {
        return new Diagram( statistics, xDimension, yDimension, durationUnits, false );
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
     * @param leadThreshold is true for one threshold, false for one lead duration
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the data has an unexpected shape
     */

    private Diagram( List<DiagramStatisticOuter> statistics,
                     MetricDimension xDimension,
                     MetricDimension yDimension,
                     ChronoUnit durationUnits,
                     boolean leadThreshold )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( xDimension );
        Objects.requireNonNull( yDimension );
        Objects.requireNonNull( durationUnits );

        int timeWindowCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata().getTimeWindow() )
                                    .size();

        int thresholdCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata().getThresholds() )
                                   .size();

        // Create the series
        List<Pair<String, Pair<DiagramStatisticComponent, DiagramStatisticComponent>>> innerStatistics =
                new ArrayList<>();

        if ( statistics.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot create a diagram chart with no statistics." );
        }

        MetricConstants metricName = statistics.get( 0 )
                                               .getMetricName();

        if ( ( leadThreshold && timeWindowCount > 1 ) || ( !leadThreshold && thresholdCount > 1 ) )
        {
            throw new IllegalArgumentException( "Unexpected data configuration for the " + metricName
                                                + ". Expected a single time window and one or more thresholds or a "
                                                + "single threshold and one or more time windows, but found "
                                                + timeWindowCount
                                                + " time windows and "
                                                + thresholdCount
                                                + " thresholds." );
        }

        for ( DiagramStatisticOuter nextSeries : statistics )
        {
            String seriesName = Diagram.getSeriesName( nextSeries, durationUnits, leadThreshold );
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
     * @param durationUnits the duration units
     * @param leadThreshold is true for one lead duration, false for one threshold
     * @return the series name
     * @throws IllegalArgumentException if the counts are not supported
     */

    private static String getSeriesName( DiagramStatisticOuter diagram,
                                         ChronoUnit durationUnits,
                                         boolean leadThreshold )
    {
        String label;

        // One lead duration and one or more thresholds: label by threshold
        if ( leadThreshold )
        {
            // Qualifier for dimensions that are repeated, such as quantile curves in an ensemble QQ diagram
            String qualifier = diagram.getStatistic()
                                      .getStatistics( 0 )
                                      .getName();

            // If there is a qualifier, then there is a single threshold and up to N named components, so only use the 
            // qualifier
            if ( !qualifier.isBlank() )
            {
                label = qualifier;
            }
            else
            {
                label = DataUtilities.toStringWithoutUnits( diagram.getPoolMetadata()
                                                                   .getThresholds() );
            }
        }
        // One threshold and one or more lead durations: label by lead duration
        else
        {
            Number numericDuration = DataUtilities.durationToNumericUnits( diagram.getPoolMetadata()
                                                                                  .getTimeWindow()
                                                                                  .getLatestLeadDuration(),
                                                                           durationUnits );
            label = numericDuration.toString();
        }

        return label;
    }

}

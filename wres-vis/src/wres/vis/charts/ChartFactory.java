package wres.vis.charts;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontFormatException;
import java.awt.GraphicsEnvironment;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.Line2D;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.LegendItem;
import org.jfree.chart.LegendItemCollection;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.annotations.XYAnnotation;
import org.jfree.chart.annotations.XYLineAnnotation;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.CombinedDomainXYPlot;
import org.jfree.chart.plot.DefaultDrawingSupplier;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.chart.renderer.xy.XYErrorRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.chart.title.TextTitle;
import org.jfree.chart.ui.RectangleInsets;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.config.MetricConstants.MetricGroup;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentType;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.vis.data.ChartDataFactory;

/**
 * <p>Factory to create a chart from a chart dataset.
 *
 * <p>TODO: eliminate all references to specific metrics from the charting process. There are 2-3 instances left with
 * associated TODOs.
 *
 * @author James Brown
 * @author Hank Herr
 */
public class ChartFactory
{
    /**
     * The chart type.
     */

    public enum ChartType
    {
        /** Not one of the other types, unique. */
        UNIQUE,
        /** Arranged by lead duration and then threshold. */
        LEAD_THRESHOLD,
        /** Arranged by threshold and then lead duration. */
        THRESHOLD_LEAD,
        /** Pooling windows. */
        POOLING_WINDOW, // Internal type only, not declared
        /** Timing error summary statistics. */
        TIMING_ERROR_SUMMARY_STATISTICS // Internal type only, not declared
    }

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ChartFactory.class );

    /** A set of diagram statistic types that do not have lead duration axes, including sub-categories. */
    private static final Set<StatisticType> DIAGRAMS_WITHOUT_LEAD_AXIS = Set.of( StatisticType.DIAGRAM,
                                                                                 StatisticType.BOXPLOT_PER_PAIR );

    /** Grid paint. */
    private static final Paint GRID_PAINT = new Color( 225, 225, 225 );

    /** Default font name. */
    private static final String DEFAULT_FONT_NAME = "verdana";

    /** Default chart font if the preferred font cannot be found. */
    private static final Font DEFAULT_CHART_FONT = new Font( DEFAULT_FONT_NAME, Font.PLAIN, 10 );

    /** Default chart title font if the preferred font cannot be found. */
    private static final Font DEFAULT_CHART_TITLE_FONT = new Font( DEFAULT_FONT_NAME, Font.PLAIN, 11 );

    /** The default length of the error bars. */
    private static final int DEFAULT_ERROR_BAR_CAP_LENGTH = 5;

    /** Series colors. */
    private final Color[] seriesColors;

    /** Chart font. */
    private final Font chartFont;

    /** Chart font for the main chart title. */
    private final Font chartFontTitle;

    /**
     * @return an instance
     */

    public static ChartFactory of()
    {
        return new ChartFactory();
    }

    /**
     * Creates a chart for each component of a score.
     *
     * @param statistics The metric output to plot.
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return a map of {@link JFreeChart} containing the charts
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    public Map<MetricConstants, JFreeChart> getScoreCharts( List<DoubleScoreStatisticOuter> statistics,
                                                            GraphicShape graphicShape,
                                                            ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( graphicShape );
        Objects.requireNonNull( durationUnits );

        Map<MetricConstants, JFreeChart> results = new EnumMap<>( MetricConstants.class );

        Map<MetricConstants, List<DoubleScoreComponentOuter>> slicedInput =
                Slicer.filterByMetricComponent( statistics );

        for ( Map.Entry<MetricConstants, List<DoubleScoreComponentOuter>> entry : slicedInput.entrySet() )
        {
            JFreeChart engine = this.getScoreChartForOneScoreComponent( statistics.get( 0 )
                                                                                  .getMetricName(),
                                                                        entry.getValue(),
                                                                        graphicShape,
                                                                        durationUnits );
            results.put( entry.getKey(), engine );
        }

        LOGGER.debug( "Created {} score charts, one for each of the following score components: {}.",
                      results.size(),
                      results.keySet() );

        return Collections.unmodifiableMap( results );
    }

    /**
     * Creates a chart for each component of a duration score.
     *
     * @param statistics the statistics from which to build the categorical plot
     * @param durationUnits the duration units
     * @return a {@link JFreeChart}
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    public JFreeChart getDurationScoreChart( List<DurationScoreStatisticOuter> statistics,
                                             ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

        // Find the metadata for the first element, which is sufficient here
        PoolMetadata metadata = statistics.get( 0 )
                                          .getPoolMetadata();

        MetricConstants metricName = statistics.get( 0 )
                                               .getMetricName();

        //Build the source.
        CategoryDataset source = ChartDataFactory.ofDurationScoreSummaryStatistics( statistics );

        String legendTitle = this.getLegendName( metricName,
                                                 metadata,
                                                 ChartType.TIMING_ERROR_SUMMARY_STATISTICS,
                                                 GraphicShape.DEFAULT,
                                                 durationUnits );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        String title = this.getChartTitle( metadata,
                                           Pair.of( metricName, null ),
                                           durationUnits,
                                           ChartType.TIMING_ERROR_SUMMARY_STATISTICS,
                                           StatisticType.DURATION_SCORE,
                                           null,
                                           quantiles );

        String rangeTitle = metricName.toString() + " [HOURS]";
        String domainTitle = "SUMMARY STATISTIC";

        JFreeChart chart = org.jfree.chart.ChartFactory.createBarChart( title,
                                                                        domainTitle,
                                                                        rangeTitle,
                                                                        source,
                                                                        PlotOrientation.VERTICAL,
                                                                        true,
                                                                        false,
                                                                        false );

        CategoryPlot plot = chart.getCategoryPlot();
        chart.setAntiAlias( true );
        this.setChartPadding( chart );
        this.setChartTheme( chart );
        this.setSeriesColorAndShape( plot );

        // Set the legend on/off and the title
        if ( this.setLegendVisible( chart, true ) )
        {
            this.setLegendTitle( chart, legendTitle, true );
        }

        this.setCategoryPlotAxes( chart.getCategoryPlot() );

        return chart;
    }

    /**
     * Returns a collection of verification diagrams.
     * @param statistics the statistics
     * @param graphicShape the shape of the graphic to plot
     * @param durationUnits the duration units
     * @return a map of diagrams by data slice key
     * @throws ChartBuildingException If the chart fails to construct for any reason
     */
    public Map<Object, JFreeChart> getDiagramCharts( List<DiagramStatisticOuter> statistics,
                                                     GraphicShape graphicShape,
                                                     ChronoUnit durationUnits )
    {
        ConcurrentMap<Object, JFreeChart> results = new ConcurrentSkipListMap<>();

        // Find the metadata for the first element, which is sufficient here
        DiagramStatisticOuter first = statistics.get( 0 );
        PoolMetadata metadata = first.getPoolMetadata();
        MetricConstants metricName = first.getMetricName();
        DiagramMetricComponent domain = first.getComponent( DiagramComponentType.PRIMARY_DOMAIN_AXIS );
        DiagramMetricComponent range = first.getComponent( DiagramComponentType.PRIMARY_RANGE_AXIS );
        MetricDimension domainDimension = first.getComponentName( DiagramComponentType.PRIMARY_DOMAIN_AXIS );
        MetricDimension rangeDimension = first.getComponentName( DiagramComponentType.PRIMARY_RANGE_AXIS );
        boolean hasDiagonal = first.getStatistic()
                                   .getMetric()
                                   .getHasDiagonal();

        EnsembleAverageType ensembleAverageType = metadata.getPool()
                                                          .getEnsembleAverageType();

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, graphicShape );

        LOGGER.debug( "Creating a diagram chart for metric {} with a chart type of {}, a domain dimension of {}, a "
                      + "range dimension of {} and a diagonal line status of: {}.",
                      metricName,
                      chartType,
                      domainDimension,
                      rangeDimension,
                      hasDiagonal );

        // Determine the key for each chart, time window by default
        Set<Object> keySetValues = this.getKeysForSlicingDiagrams( statistics, chartType );

        String rangeTitle = rangeDimension + " [" + range.getUnits() + "]";
        String domainTitle = domainDimension + " [" + domain.getUnits() + "]";
        String legendTitle = this.getLegendName( metricName, metadata, chartType, graphicShape, durationUnits );

        // One chart per key instance
        for ( Object keyInstance : keySetValues )
        {
            // Slice the statistics for the diagram
            List<DiagramStatisticOuter> slicedStatistics =
                    ChartDataFactory.getSlicedStatisticsForDiagram( keyInstance,
                                                                    statistics,
                                                                    chartType );

            List<PoolMetadata> metadatas = slicedStatistics.stream()
                                                           .map( DiagramStatisticOuter::getPoolMetadata )
                                                           .toList();

            PoolMetadata union = PoolSlicer.unionOf( metadatas );

            SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

            String title = this.getChartTitle( union,
                                               Pair.of( metricName, null ),
                                               durationUnits,
                                               chartType,
                                               StatisticType.DIAGRAM,
                                               ensembleAverageType,
                                               quantiles );

            XYDataset dataset;

            // One lead duration and up to many thresholds
            if ( chartType == ChartType.LEAD_THRESHOLD || chartType == ChartType.POOLING_WINDOW )
            {
                dataset = ChartDataFactory.ofVerificationDiagramByLeadAndThreshold( slicedStatistics,
                                                                                    domainDimension,
                                                                                    rangeDimension,
                                                                                    durationUnits );
            }
            // One threshold and up to many lead durations
            else
            {
                dataset = ChartDataFactory.ofVerificationDiagramByThresholdAndLead( slicedStatistics,
                                                                                    domainDimension,
                                                                                    rangeDimension,
                                                                                    durationUnits );
            }

            JFreeChart chart;

            // The reliability diagram is a special case, combining two plots
            if ( metricName == MetricConstants.RELIABILITY_DIAGRAM )
            {
                // Do NOT set the chart theme for the combined plot because it returns the series renderer to default. 
                // This is probably a bug in JFreeChart, since the behavior is not seen for other chart types. Instead, 
                // set the chart theme components manually for the reliability diagram
                chart = this.getReliabilityDiagram( title,
                                                    slicedStatistics,
                                                    durationUnits,
                                                    chartType == ChartType.LEAD_THRESHOLD
                                                    || chartType == ChartType.POOLING_WINDOW );
            }
            else
            {
                chart = org.jfree.chart.ChartFactory.createXYLineChart( title,
                                                                        domainTitle,
                                                                        rangeTitle,
                                                                        dataset,
                                                                        PlotOrientation.VERTICAL,
                                                                        true,
                                                                        false,
                                                                        false );

                XYPlot plot = chart.getXYPlot();
                plot.setBackgroundPaint( Color.WHITE );

                // Add a diagonal, as needed
                if ( hasDiagonal )
                {
                    this.addDiagonalLine( plot );
                }

                this.setXYPlotAxes( plot,
                                    domain.getMinimum(),
                                    domain.getMaximum(),
                                    range.getMinimum(),
                                    range.getMaximum(),
                                    hasDiagonal, // Plots with a diagonal are always square, plots without, probably not
                                    !hasDiagonal ); // Plots with a diagonal should not show a zero range marker

                // Set the renderer
                this.setChartTheme( chart );
                this.setSeriesColorAndShape( plot, !quantiles.isEmpty() );
            }

            chart.setAntiAlias( true );
            chart.setBackgroundPaint( Color.WHITE );
            this.setChartPadding( chart );

            // Set the legend on/off and the title
            if ( this.setLegendVisible( chart, false ) )
            {
                this.setLegendTitle( chart, legendTitle, false );
            }

            results.put( keyInstance, chart );
        }

        LOGGER.debug( "Created {} diagram charts for metric {}, one chart for each of the following chart keys: {}.",
                      metricName,
                      results.size(),
                      results.keySet() );

        return results;
    }

    /**
     * Creates a chart for one diagram that plots timing error statistics.
     *
     * @param statistics the metric output to plot 
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    public JFreeChart getDurationDiagramChart( List<DurationDiagramStatisticOuter> statistics,
                                               ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

        // Find the metadata for the first element, which is sufficient here
        PoolMetadata metadata = statistics.get( 0 )
                                          .getPoolMetadata();

        // Component name
        MetricConstants metricName = statistics.get( 0 )
                                               .getMetricName();

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, GraphicShape.DEFAULT );

        //Build the source.
        XYDataset source = ChartDataFactory.ofDurationDiagramStatistics( statistics );

        String legendTitle = this.getLegendName( metricName, metadata, chartType, GraphicShape.DEFAULT, durationUnits );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        String title = this.getChartTitle( metadata,
                                           Pair.of( metricName, null ),
                                           durationUnits,
                                           chartType,
                                           StatisticType.DURATION_DIAGRAM,
                                           null,
                                           quantiles );

        String rangeTitle = metricName.toString() + " [HOURS]";
        String domainTitle = "FORECAST ISSUE TIME [UTC]";

        JFreeChart chart = org.jfree.chart.ChartFactory.createTimeSeriesChart( title,
                                                                               domainTitle,
                                                                               rangeTitle,
                                                                               source,
                                                                               true,
                                                                               false,
                                                                               false );

        // Set the date/time format
        DateAxis dateAxis = ( DateAxis ) chart.getXYPlot()
                                              .getDomainAxis();
        dateAxis.setDateFormatOverride( new SimpleDateFormat( "yyyy-MM-dd+HH" ) );

        XYPlot plot = chart.getXYPlot();

        chart.setAntiAlias( true );
        this.setChartTheme( chart );
        this.setChartPadding( chart );
        this.setSeriesColorAndShape( plot, !quantiles.isEmpty() );

        // Set the legend on/off and the title
        if ( this.setLegendVisible( chart, false ) )
        {
            this.setLegendTitle( chart, legendTitle, false );
        }

        this.setXYPlotAxes( chart.getXYPlot(), 0, 0, 0, 0, false, true ); // Autofit axes

        return chart;
    }

    /**
     * Creates a box plot chart containing data for several pools.
     *
     * @param statistics the metric output to plot
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance
     * @throws ChartBuildingException if the chart fails to build for any reason
     */
    public JFreeChart getBoxplotChart( List<BoxplotStatisticOuter> statistics,
                                       ChronoUnit durationUnits )
    {
        // Find the metadata for the first element, which is sufficient here
        BoxplotStatisticOuter first = statistics.get( 0 );

        MetricConstants metricName = first.getMetricName();
        PoolMetadata metadata = first.getPoolMetadata();
        BoxplotMetric metric = first.getStatistic()
                                    .getMetric();
        QuantileValueType type = metric.getQuantileValueType();
        String metricUnits = metric.getUnits();
        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        EnsembleAverageType ensembleAverageType = metadata.getPool()
                                                          .getEnsembleAverageType();

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, GraphicShape.DEFAULT );

        // Build the source
        XYDataset source = ChartDataFactory.ofBoxplotStatistics( statistics, durationUnits );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        String title = this.getChartTitle( metadata,
                                           Pair.of( metricName, null ),
                                           durationUnits,
                                           chartType,
                                           metricName.getMetricOutputGroup(),
                                           ensembleAverageType,
                                           quantiles );

        String rangeTitle = type.toString()
                                .replace( "_", " " )
                            + " ["
                            + metricUnits
                            + "]";

        String domainTitle = "LATEST FORECAST LEAD TIME [" + leadUnits + "]";

        JFreeChart chart = org.jfree.chart.ChartFactory.createXYLineChart( title,
                                                                           domainTitle,
                                                                           rangeTitle,
                                                                           source,
                                                                           PlotOrientation.VERTICAL,
                                                                           true,
                                                                           false,
                                                                           false );

        // To quote the documentation, this setting "usually" improve the appearance of charts. However, experimentation 
        // indicates that it reduces the quality of the box plots
        chart.setAntiAlias( false );

        chart.removeLegend();

        XYPlot plot = chart.getXYPlot();
        plot.setRenderer( this.getBoxPlotRenderer() );

        this.setChartTheme( chart );
        this.setDomainAxisForLeadDurations( plot );
        this.setChartPadding( chart );
        this.setXYPlotAxes( plot, 0, 0, 0, 0, false, true ); // Autofit axes

        return chart;
    }

    /**
     * Creates a box plot chart for each pool.
     *
     * @param statistics the metric output to plot
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance for each pool
     * @throws ChartBuildingException if the chart fails to build for any reason
     */
    public Map<TimeWindowOuter, JFreeChart> getBoxplotChartPerPool( List<BoxplotStatisticOuter> statistics,
                                                                    ChronoUnit durationUnits )
    {
        Map<TimeWindowOuter, JFreeChart> results = new ConcurrentSkipListMap<>();

        // Find the metadata for the first element, which is sufficient here
        BoxplotStatisticOuter first = statistics.get( 0 );
        MetricConstants metricName = first.getMetricName();
        BoxplotMetric metric = first.getStatistic()
                                    .getMetric();
        LinkedValueType valueType = metric.getLinkedValueType();
        String valueUnits = metric.getUnits();

        if ( !metricName.isInGroup( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            throw new ChartBuildingException( "Unrecognized type of statistics for box plot graphic. Expected one of "
                                              + StatisticType.BOXPLOT_PER_PAIR
                                              + ", but got "
                                              + metricName.getMetricOutputGroup()
                                              + "." );
        }

        //For each input in the list, create a chart
        for ( BoxplotStatisticOuter next : statistics )
        {
            // Skip empty outputs: #65503
            if ( next.getStatistic().getStatisticsCount() != 0 )
            {
                List<BoxplotStatisticOuter> nextStatistics = List.of( next );
                JFreeChart chart = this.getBoxplotChartPerPool( nextStatistics, valueType, valueUnits, durationUnits );

                TimeWindowOuter timeWindow = next.getPoolMetadata()
                                                 .getTimeWindow();

                results.put( timeWindow, chart );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Skipped the box plot outputs for {} because there were no box plot statistics to draw.",
                              next.getPoolMetadata() );
            }
        }

        LOGGER.debug( "Created {} box plot charts for metric {}, one chart for each of the following time windows: {}.",
                      metricName,
                      results.size(),
                      results.keySet() );

        return Collections.unmodifiableMap( results );
    }

    /**
     * Creates a box plot chart containing data for one pool.
     *
     * @param statistics the metric output to plot
     * @param valueType the type of linked value for the domain axis
     * @param valueUnits the type of value units for the domain axis
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    private JFreeChart getBoxplotChartPerPool( List<BoxplotStatisticOuter> statistics,
                                               LinkedValueType valueType,
                                               String valueUnits,
                                               ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( valueType );
        Objects.requireNonNull( valueUnits );
        Objects.requireNonNull( durationUnits );

        // Find the metadata for the first element, which is sufficient here
        BoxplotStatisticOuter first = statistics.get( 0 );

        MetricConstants metricName = first.getMetricName();
        PoolMetadata metadata = first.getPoolMetadata();
        BoxplotMetric metric = first.getStatistic()
                                    .getMetric();
        QuantileValueType type = metric.getQuantileValueType();
        String metricUnits = metric.getUnits();

        EnsembleAverageType ensembleAverageType = metadata.getPool()
                                                          .getEnsembleAverageType();

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, GraphicShape.DEFAULT );

        // Build the source
        XYDataset source = ChartDataFactory.ofBoxplotStatistics( statistics, durationUnits );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        String title = this.getChartTitle( metadata,
                                           Pair.of( metricName, null ),
                                           durationUnits,
                                           chartType,
                                           metricName.getMetricOutputGroup(),
                                           ensembleAverageType,
                                           quantiles );

        // Create the titles
        String domainTitle = valueType.toString()
                                      .replace( "_", " " )
                             + " ["
                             + valueUnits
                             + "]";

        String rangeTitle = type.toString()
                                .replace( "_", " " )
                            + " ["
                            + metricUnits
                            + "]";

        JFreeChart chart = org.jfree.chart.ChartFactory.createXYLineChart( title,
                                                                           domainTitle,
                                                                           rangeTitle,
                                                                           source,
                                                                           PlotOrientation.VERTICAL,
                                                                           true,
                                                                           false,
                                                                           false );

        // To quote the documentation, this setting "usually" improve the appearance of charts. However, experimentation 
        // indicates that it reduces the quality of the box plots
        chart.setAntiAlias( false );

        chart.removeLegend();

        XYPlot plot = chart.getXYPlot();
        plot.setRenderer( this.getBoxPlotRenderer() );

        this.setChartTheme( chart );
        this.setChartPadding( chart );
        this.setXYPlotAxes( plot, 0, 0, 0, 0, false, true ); // Autofit axes

        return chart;
    }

    /**
     * Creates a chart for one score component.
     *
     * @param metricName the metric name
     * @param statistics the metric output to plot 
     * @param graphicShape the shape of the graphic to plot
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance
     * @throws ChartBuildingException if the chart fails to build
     */
    private JFreeChart getScoreChartForOneScoreComponent( MetricConstants metricName,
                                                          List<DoubleScoreComponentOuter> statistics,
                                                          GraphicShape graphicShape,
                                                          ChronoUnit durationUnits )
    {
        // Find the metadata for the first element, which is sufficient here
        PoolMetadata metadata = statistics.get( 0 )
                                          .getPoolMetadata();
        String metricUnits = statistics.get( 0 )
                                       .getStatistic()
                                       .getMetric()
                                       .getUnits();

        // Component name
        MetricConstants metricComponentName = statistics.get( 0 )
                                                        .getMetricName();

        EnsembleAverageType ensembleAverageType = metadata.getPool()
                                                          .getEnsembleAverageType();

        // So not qualify a "main" score component because it is qualified by the overall metric name
        if ( metricComponentName == MetricConstants.MAIN )
        {
            metricComponentName = null;
        }

        // Determine the chart type
        ChartType chartType = ChartFactory.getChartType( metricName, graphicShape );

        String thresholdUnits = metadata.getEvaluation()
                                        .getMeasurementUnit();

        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        String legendTitle = this.getLegendName( metricName, metadata, chartType, graphicShape, durationUnits );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        String title = this.getChartTitle( metadata,
                                           Pair.of( metricName, metricComponentName ),
                                           durationUnits,
                                           chartType,
                                           StatisticType.DOUBLE_SCORE,
                                           ensembleAverageType,
                                           quantiles );
        String rangeTitle = metricName.toString()
                            + " ["
                            + metricUnits
                            + "]";

        JFreeChart chart;


        // Build the source
        XYDataset source;

        // Lead duration and then threshold
        if ( chartType == ChartType.LEAD_THRESHOLD )
        {
            source = ChartDataFactory.ofDoubleScoreByLeadAndThreshold( statistics, durationUnits );
            String domainTitle = "LATEST FORECAST LEAD TIME [" + leadUnits + "]";
            chart = org.jfree.chart.ChartFactory.createXYLineChart( title,
                                                                    domainTitle,
                                                                    rangeTitle,
                                                                    source,
                                                                    PlotOrientation.VERTICAL,
                                                                    true,
                                                                    false,
                                                                    false );

            XYPlot plot = chart.getXYPlot();
            this.setDomainAxisForLeadDurations( plot );
        }
        // Threshold and then lead duration
        else if ( chartType == ChartType.THRESHOLD_LEAD )
        {
            source = ChartDataFactory.ofDoubleScoreByThresholdAndLead( statistics, durationUnits );
            String domainTitle = "THRESHOLD VALUE [" + thresholdUnits + "]";
            chart = org.jfree.chart.ChartFactory.createXYLineChart( title,
                                                                    domainTitle,
                                                                    rangeTitle,
                                                                    source,
                                                                    PlotOrientation.VERTICAL,
                                                                    true,
                                                                    false,
                                                                    false );
        }
        // Pooling windows
        else if ( chartType == ChartType.POOLING_WINDOW )
        {
            source = ChartDataFactory.ofDoubleScoreByPoolingWindow( statistics, durationUnits, graphicShape );
            String domainTitle;
            if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS )
            {
                domainTitle = "TIME AT CENTER OF ISSUED TIME WINDOW [UTC]";
            }
            else
            {
                domainTitle = "TIME AT CENTER OF VALID TIME WINDOW [UTC]";
            }

            chart = org.jfree.chart.ChartFactory.createTimeSeriesChart( title,
                                                                        domainTitle,
                                                                        rangeTitle,
                                                                        source,
                                                                        true,
                                                                        false,
                                                                        false );
            // Set the date/time format
            DateAxis dateAxis = ( DateAxis ) chart.getXYPlot()
                                                  .getDomainAxis();
            dateAxis.setDateFormatOverride( new SimpleDateFormat( "yyyy-MM-dd+HH" ) );
        }
        else
        {
            throw new IllegalArgumentException( "Chart type of " + chartType
                                                + " is not valid for a generic scalar output plot; must be one of "
                                                + ChartType.LEAD_THRESHOLD
                                                + ", "
                                                + ChartType.THRESHOLD_LEAD
                                                + ", "
                                                + ChartType.POOLING_WINDOW
                                                + "." );
        }

        chart.setAntiAlias( true );
        this.setChartPadding( chart );
        this.setChartTheme( chart );
        XYPlot plot = chart.getXYPlot();
        this.setSeriesColorAndShape( plot, !quantiles.isEmpty() );

        // Set the legend on/off and the title
        if ( this.setLegendVisible( chart, false ) )
        {
            this.setLegendTitle( chart, legendTitle, false );
        }

        this.setXYPlotAxes( chart.getXYPlot(), 0, 0, 0, 0, false, true ); // Autofit axes

        return chart;
    }

    /**
     * Creates a reliability diagram.
     * @param title the plot title
     * @param statistics the statistics
     * @param durationUnits the duration units
     * @param leadThreshold is true for a diagram with one lead time and up to many thresholds, false for the reverse
     * @return the reliability diagram
     */

    private JFreeChart getReliabilityDiagram( String title,
                                              List<DiagramStatisticOuter> statistics,
                                              ChronoUnit durationUnits,
                                              boolean leadThreshold )
    {
        Objects.requireNonNull( title );
        Objects.requireNonNull( durationUnits );
        Objects.requireNonNull( statistics );

        XYDataset sampleSize;
        XYDataset reliability;

        if ( leadThreshold )
        {
            sampleSize = ChartDataFactory.ofVerificationDiagramByLeadAndThreshold( statistics,
                                                                                   MetricDimension.FORECAST_PROBABILITY,
                                                                                   MetricDimension.SAMPLE_SIZE,
                                                                                   durationUnits );
            reliability = ChartDataFactory.ofVerificationDiagramByLeadAndThreshold( statistics,
                                                                                    MetricDimension.FORECAST_PROBABILITY,
                                                                                    MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                                    durationUnits );
        }
        else
        {
            sampleSize = ChartDataFactory.ofVerificationDiagramByThresholdAndLead( statistics,
                                                                                   MetricDimension.FORECAST_PROBABILITY,
                                                                                   MetricDimension.SAMPLE_SIZE,
                                                                                   durationUnits );
            reliability = ChartDataFactory.ofVerificationDiagramByThresholdAndLead( statistics,
                                                                                    MetricDimension.FORECAST_PROBABILITY,
                                                                                    MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                                    durationUnits );
        }

        ValueAxis domainAxis = new NumberAxis( MetricDimension.FORECAST_PROBABILITY.toString() );
        ValueAxis primaryRangeAxis = new NumberAxis( MetricDimension.OBSERVED_RELATIVE_FREQUENCY.toString() );
        ValueAxis secondaryRangeAxis = new NumberAxis( MetricDimension.SAMPLE_SIZE.toString() );

        // Set the axis font
        Font font = this.getChartFont();

        domainAxis.setLabelFont( font );
        domainAxis.setTickLabelFont( font );
        primaryRangeAxis.setLabelFont( font );
        secondaryRangeAxis.setLabelFont( font );
        primaryRangeAxis.setTickLabelFont( font );
        secondaryRangeAxis.setTickLabelFont( font );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );
        XYPlot reliabilityPlot = new XYPlot( reliability, domainAxis, primaryRangeAxis, null );
        this.setXYPlotAxes( reliabilityPlot, 0, 1, 0, 1, true, false );
        this.addDiagonalLine( reliabilityPlot );
        this.setSeriesColorAndShape( reliabilityPlot, !quantiles.isEmpty() );

        XYPlot sampleSizePlot = new XYPlot( sampleSize, domainAxis, secondaryRangeAxis, null );
        this.setXYPlotAxes( sampleSizePlot, 0, 1, 0, 0, false, false );

        // The reliability plot controls the legend, so remove legend items from the sample size plot
        LegendItemCollection noLegendItems = new LegendItemCollection();
        sampleSizePlot.setFixedLegendItems( noLegendItems );
        this.setSeriesColorAndShape( sampleSizePlot, false );

        CombinedDomainXYPlot combinedPlot = new CombinedDomainXYPlot( domainAxis );
        combinedPlot.setGap( 5.0 );

        combinedPlot.add( reliabilityPlot, 8 );
        combinedPlot.add( sampleSizePlot, 2 );
        combinedPlot.setOrientation( PlotOrientation.VERTICAL );

        Font titleFont = this.getChartTitleFont();
        JFreeChart chart = new JFreeChart( title, titleFont, combinedPlot, true );

        chart.getLegend()
             .setItemFont( font );

        return chart;
    }

    /**
     * Sets the chart theme
     *
     * @param chart the chart
     * @throws NullPointerException if either input is null
     */

    private void setChartTheme( JFreeChart chart )
    {
        Objects.requireNonNull( chart );

        Font font = this.getChartFont();
        Font fontTitle = this.getChartTitleFont();

        StandardChartTheme theme = new StandardChartTheme( "wres" );

        theme.setLargeFont( font );
        theme.setRegularFont( font );
        theme.setSmallFont( font );
        theme.setExtraLargeFont( fontTitle );

        theme.setPlotBackgroundPaint( Color.WHITE );
        theme.setChartBackgroundPaint( Color.WHITE );

        theme.setDomainGridlinePaint( ChartFactory.GRID_PAINT );
        theme.setRangeGridlinePaint( ChartFactory.GRID_PAINT );

        // Apply the theme
        theme.apply( chart );
    }

    /**
     * Sets the XY plot axes. When the minimum and maximum values are equal for a dimension, that is considered unset.
     * @param plot the plot
     * @param minimumDomain the minimum domain axis value
     * @param maximumDomain the maximum domain axis value
     * @param minimumRange the minimum range axis value
     * @param maximumRange the maximum range axis value
     * @param isSquare is true if the plot should be square, regardless of the prescribed minimum and maximum values
     * @param hasZeroRangeMarker is true if the plot should have a zero range marker, false for none
     */

    private void setXYPlotAxes( XYPlot plot,
                                double minimumDomain,
                                double maximumDomain,
                                double minimumRange,
                                double maximumRange,
                                boolean isSquare,
                                boolean hasZeroRangeMarker )
    {
        plot.setRangeZeroBaselineVisible( hasZeroRangeMarker );

        ValueAxis domainAxis = plot.getDomainAxis();
        domainAxis.setAxisLineVisible( false );

        NumberAxis rangeAxis = ( NumberAxis ) plot.getRangeAxis();
        rangeAxis.setAxisLineVisible( false );
        rangeAxis.setAutoRangeIncludesZero( true );

        // Set the axes limits
        double lowerDomain = domainAxis.getLowerBound();
        double upperDomain = domainAxis.getUpperBound();
        double lowerRange = rangeAxis.getLowerBound();
        double upperRange = rangeAxis.getUpperBound();

        // Equality precision
        double epsilon = 0.00001;

        // A valid domain interval was prescribed?
        if ( Math.abs( minimumDomain - maximumDomain ) > epsilon )
        {
            if ( Double.isFinite( minimumDomain ) )
            {
                lowerDomain = minimumDomain;
            }
            if ( Double.isFinite( maximumDomain ) )
            {
                upperDomain = maximumDomain;
            }
        }

        // A valid range interval was prescribed?
        if ( Math.abs( minimumRange - maximumRange ) > epsilon )
        {
            if ( Double.isFinite( minimumRange ) )
            {
                lowerRange = minimumRange;
            }
            if ( Double.isFinite( maximumRange ) )
            {
                upperRange = maximumRange;
            }
        }

        // Is square? This builds upon, but overrides, other settings
        if ( isSquare )
        {
            double lowerMost = Math.min( lowerDomain, lowerRange );
            double upperMost = Math.max( upperDomain, upperRange );

            lowerDomain = lowerMost;
            lowerRange = lowerMost;
            upperDomain = upperMost;
            upperRange = upperMost;
        }

        LOGGER.debug( "Setting domain axis limits to [{},{}] and range axis limits to [{},{}]. The original domain "
                      + "axis limits were [{},{}] and the range axis limits were [{},{}].",
                      lowerDomain,
                      upperDomain,
                      lowerRange,
                      upperRange,
                      domainAxis.getLowerBound(),
                      domainAxis.getUpperBound(),
                      rangeAxis.getLowerBound(),
                      rangeAxis.getUpperBound() );

        rangeAxis.setLowerBound( lowerRange );
        rangeAxis.setUpperBound( upperRange );
        domainAxis.setLowerBound( lowerDomain );
        domainAxis.setUpperBound( upperDomain );
    }

    /**
     * Sets the plot axes of a category plot.
     * @param plot the category plot
     */

    private void setCategoryPlotAxes( CategoryPlot plot )
    {
        plot.setRangeZeroBaselineVisible( true );

        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setAxisLineVisible( false );

        // To ensure the labels are fully displayed, ideally on one line
        domainAxis.setMaximumCategoryLabelWidthRatio( 1.5F );
        domainAxis.setMaximumCategoryLabelLines( 2 );

        // Shrink the gaps between categories
        domainAxis.setCategoryMargin( 0.1 );

        plot.setDomainGridlinesVisible( true );

        NumberAxis rangeAxis = ( NumberAxis ) plot.getRangeAxis();
        rangeAxis.setAxisLineVisible( false );
        rangeAxis.setAutoRangeIncludesZero( true );
    }

    /**
     * Adds a diagonal line to the plot.
     * @param plot the plot
     */

    private void addDiagonalLine( XYPlot plot )
    {
        // Limit practically because XYAnnotation will not render a line with infinite bounds
        double min = Math.min( plot.getDomainAxis()
                                   .getLowerBound(),
                               plot.getRangeAxis()
                                   .getLowerBound() );
        double max = Math.max( plot.getDomainAxis()
                                   .getUpperBound(),
                               plot.getRangeAxis()
                                   .getUpperBound() );

        XYAnnotation annotation = new XYLineAnnotation( min,
                                                        min,
                                                        max,
                                                        max,
                                                        new BasicStroke(),
                                                        Color.BLACK );
        plot.addAnnotation( annotation );
    }

    /**
     * Sets the domain axis for lead durations in decimal units.
     * @param plot the plot
     * @throws NullPointerException if the input is null
     */

    private void setDomainAxisForLeadDurations( XYPlot plot )
    {
        Objects.requireNonNull( plot );

        // Use decimal notation for the domain axis labels with up to 5 d.p.
        DecimalFormat newFormat = new DecimalFormat( "#.#" );
        newFormat.setMaximumFractionDigits( 5 );
        NumberAxis domainAxis = ( NumberAxis ) plot.getDomainAxis();
        domainAxis.setNumberFormatOverride( newFormat );
    }

    /**
     * Sets the chart padding.
     * @param chart the chart
     * @throws NullPointerException if the chart is null
     */

    private void setChartPadding( JFreeChart chart )
    {
        Objects.requireNonNull( chart );

        RectangleInsets chartPadding = new RectangleInsets( 0F, -10F, 0F, 20F );
        RectangleInsets titlePadding = new RectangleInsets( 0F, 40F, 0F, 0F );
        RectangleInsets legendPadding = new RectangleInsets( 0F, 30F, 0F, 0F );

        chart.setPadding( chartPadding );

        TextTitle title = chart.getTitle();
        if ( Objects.nonNull( title ) )
        {
            title.setPadding( titlePadding );
        }

        LegendTitle legend = chart.getLegend();
        if ( Objects.nonNull( legend ) )
        {
            legend.setPadding( legendPadding );
        }
    }

    /**
     * Sets the color and shape for each series.
     *
     * @param plot the plot
     * @param errorBars is true to plot error bars, false otherwise
     * @throws NullPointerException if the plot is null
     */

    private void setSeriesColorAndShape( XYPlot plot, boolean errorBars )
    {
        Objects.requireNonNull( plot );

        Color[] colors = this.getSeriesColors();

        // Too many series for the default color sequence? Generate a sequence instead
        int seriesCount = plot.getSeriesCount();
        if ( colors.length < seriesCount )
        {
            colors = GraphicsUtils.getColorPalette( seriesCount, Color.BLUE, Color.GREEN, Color.RED );
        }

        XYLineAndShapeRenderer renderer;
        if ( errorBars )
        {
            XYErrorRenderer errors = new XYErrorRenderer();
            errors.setDrawXError( false );
            errors.setDrawYError( true );
            errors.setCapLength( ChartFactory.DEFAULT_ERROR_BAR_CAP_LENGTH );
            renderer = errors;
        }
        else
        {
            renderer = new XYLineAndShapeRenderer();
        }

        Shape[] shapes = DefaultDrawingSupplier.DEFAULT_SHAPE_SEQUENCE;

        for ( int i = 0; i < seriesCount; i++ )
        {
            renderer.setSeriesPaint( i, colors[i] );
            Shape shape = shapes[i % shapes.length];
            renderer.setSeriesShape( i, shape );
            renderer.setSeriesShapesVisible( i, true );
            renderer.setSeriesShapesFilled( i, true );
            renderer.setSeriesLinesVisible( i, true );
        }

        plot.setRenderer( renderer );
    }

    /**
     * Sets the color and shape for each series.
     *
     * @param plot the category plot
     * @throws NullPointerException if the plot is null
     */

    private void setSeriesColorAndShape( CategoryPlot plot )
    {
        Objects.requireNonNull( plot );

        BarRenderer renderer = new BarRenderer();

        // Solid bar colors
        renderer.setBarPainter( new StandardBarPainter() );

        // No padding between bars
        renderer.setItemMargin( 0 );

        // No shadow
        renderer.setShadowVisible( false );

        Color[] colors = this.getSeriesColors();

        // Too many series for the default color sequence? Generate a sequence instead.
        int seriesCount = plot.getDataset()
                              .getRowCount();

        if ( colors.length < seriesCount )
        {
            colors = GraphicsUtils.getColorPalette( seriesCount, Color.BLUE, Color.GREEN, Color.RED );
        }

        for ( int i = 0; i < seriesCount; i++ )
        {
            renderer.setSeriesPaint( i, colors[i] );
        }

        plot.setRenderer( renderer );
    }

    /**
     * Sets legend on when there is a sufficiently small number of legend items (currently, fewer than 101).
     *
     * @param chart the chart
     * @param isCategoryPlot is true for a category plot, false for an XY plot
     * @throws NullPointerException if either input is null
     */

    private boolean setLegendVisible( JFreeChart chart, boolean isCategoryPlot )
    {
        Objects.requireNonNull( chart );

        int itemCount;

        if ( isCategoryPlot )
        {
            itemCount = chart.getCategoryPlot()
                             .getLegendItems()
                             .getItemCount();
        }
        else
        {
            itemCount = chart.getXYPlot()
                             .getLegendItems()
                             .getItemCount();
        }

        // Too many items? Then remove the legend
        if ( itemCount > 100 )
        {
            LOGGER.debug( "Removing legend from chart entitled '{}' because there are too many legend items to "
                          + "display: {}.",
                          chart.getTitle().getText(),
                          itemCount );

            chart.removeLegend();

            return false;
        }

        return true;
    }

    /**
     * Sets the flush legend title of a chart by adding a new legend item.
     *
     * @param chart the chart
     * @param legendTitle the legend title
     * @param isCategoryPlot is true for a category plot, false for an XY plot
     * @throws NullPointerException if either input is null
     */

    private void setLegendTitle( JFreeChart chart, String legendTitle, boolean isCategoryPlot )
    {
        Objects.requireNonNull( chart );
        Objects.requireNonNull( legendTitle );

        // Create the default item
        LegendItem legendItem =
                new LegendItem( legendTitle, null, null, null, new Line2D.Double( 0, 0, 0, 0 ), Color.BLACK );
        LegendItemCollection newLegendItems = new LegendItemCollection();
        newLegendItems.add( legendItem );

        // Get the existing items
        LegendItemCollection existingLegendItems = chart.getPlot()
                                                        .getLegendItems();

        newLegendItems.addAll( existingLegendItems );

        if ( isCategoryPlot )
        {
            chart.getCategoryPlot()
                 .setFixedLegendItems( newLegendItems );
        }
        else
        {
            chart.getXYPlot()
                 .setFixedLegendItems( newLegendItems );
        }
    }

    /**
     * Creates a chart title.
     *
     * @param metadata the pool metadata, required
     * @param metricNames the metric names, required
     * @param durationUnits the duration units, required
     * @param chartType the chart type, required
     * @param statisticType the type of statistics, required
     * @param ensembleAverageType the ensemble average type, optional
     * @return the chart title
     * @throws NullPointerException if any required input is null
     */

    private String getChartTitle( PoolMetadata metadata,
                                  Pair<MetricConstants, MetricConstants> metricNames,
                                  ChronoUnit durationUnits,
                                  ChartType chartType,
                                  StatisticType statisticType,
                                  EnsembleAverageType ensembleAverageType,
                                  SortedSet<Double> quantiles )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( metricNames );
        Objects.requireNonNull( durationUnits );
        Objects.requireNonNull( chartType );
        Objects.requireNonNull( statisticType );

        MetricConstants metricName = metricNames.getLeft();
        String name = metricName.toString();

        MetricConstants metricComponentName = metricNames.getRight();

        if ( Objects.nonNull( metricComponentName ) )
        {
            name += " " + metricComponentName;
        }

        // Are decision thresholds defined?
        boolean hasDecisionThresholds = metadata.hasThresholds() && metadata.getThresholds()
                                                                            .hasTwo();

        // Qualify the ensemble average type if present and the metric is single-valued, unless it is a sample size or 
        // a univariate metric applied to the LHS of the pairs. If the LHS eventually supports ensembles, the data 
        // types will need to be qualified in the evaluation message.
        if ( Objects.nonNull( ensembleAverageType )
             && ensembleAverageType != EnsembleAverageType.NONE
             // Single-valued metrics or dichotomous metrics defined for single-valued pairs only
             && ( metricName.isInGroup( SampleDataGroup.SINGLE_VALUED )
                  || ( metricName.isInGroup( SampleDataGroup.DICHOTOMOUS ) && !hasDecisionThresholds ) )
             && metricName != MetricConstants.SAMPLE_SIZE
             && metricComponentName != MetricConstants.LEFT )
        {
            name += " of the ENSEMBLE " + ensembleAverageType.name();
        }

        String geoName = this.getGeoNameForTitle( metadata );

        name += " at " + geoName;

        String scenarioName = this.getScenarioNameForTitle( metadata, metricName );

        if ( !scenarioName.isBlank() )
        {
            name += " for " + scenarioName + " predictions of";
        }
        else
        {
            name += " for predictions of";
        }

        String variableName = this.getVariableNameForTitle( metadata, metricName, metricComponentName );

        if ( !variableName.isBlank() )
        {
            name += " " + variableName;
        }

        String baselineScenario = this.getBaselineScenarioForTitle( metadata, metricName );

        if ( !baselineScenario.isBlank() )
        {
            name += " " + baselineScenario;
        }

        String timeScale = this.getTimeScaleForTitle( metadata, durationUnits );

        if ( !timeScale.isBlank() )
        {
            name += " with a time scale of " + timeScale;
        }

        String timeWindow = this.getTimeWindowForTitle( metadata, chartType, statisticType, durationUnits );

        if ( !timeWindow.isBlank() )
        {
            // Add a new line for the time window
            name += " and a time window that has " + timeWindow;
        }

        String threshold = this.getThresholdForTitle( metadata, chartType, statisticType );

        if ( !threshold.isBlank() )
        {
            // Add a new line for the time window
            name += " and a threshold of " + threshold;
        }

        if ( !quantiles.isEmpty() && this.canShowErrorBars( metricName, chartType ) )
        {
            double minimum = quantiles.stream()
                                      .mapToDouble( Double::doubleValue )
                                      .min()
                                      .orElse( Double.NaN );

            double maximum = quantiles.stream()
                                      .mapToDouble( Double::doubleValue )
                                      .max()
                                      .orElse( Double.NaN );

            name += " and error bars for quantiles of [" + minimum + ", " + maximum + "]";
        }

        return name;
    }

    /**
     * @param metricName the metric name
     * @param chartType the chart type
     * @return whether error bars can be displayed
     */

    private boolean canShowErrorBars( MetricConstants metricName, ChartType chartType )
    {
        return metricName.isSamplingUncertaintyAllowed()
               // Disallowed for graphics
               && !metricName.isInGroup( StatisticType.DURATION_SCORE )
               && !metricName.isInGroup( StatisticType.DURATION_DIAGRAM )
               && chartType != ChartType.POOLING_WINDOW;
    }

    /**
     * Uncovers the scenario name for the plot title.
     *
     * @param metadata the sample metadata
     * @param metric the metric name
     * @return the scenario name
     * @throws NullPointerException if the metadata or metric is null
     */

    private String getScenarioNameForTitle( PoolMetadata metadata, MetricConstants metric )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( metric );

        String scenarioName = "";

        Evaluation evaluation = metadata.getEvaluation();

        // Not univariate statistics, except the sample size
        if ( !metric.isInGroup( MetricGroup.UNIVARIATE_STATISTIC ) || metric == MetricConstants.SAMPLE_SIZE )
        {
            if ( metadata.getPool().getIsBaselinePool() )
            {
                scenarioName = evaluation.getBaselineDataName();
            }
            else if ( !evaluation.getRightDataName().isBlank() )
            {
                scenarioName = evaluation.getRightDataName();
            }
        }

        return scenarioName;
    }

    /**
     * Uncovers the variable name for the plot title.
     *
     * @param metadata the sample metadata
     * @param metric the metric name
     * @param component the metric component name
     * @return the variable name
     * @throws NullPointerException if the metadata or metric is null
     */

    private String getVariableNameForTitle( PoolMetadata metadata, MetricConstants metric, MetricConstants component )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( metric );

        String variableName = "";

        Evaluation evaluation = metadata.getEvaluation();

        // Not univariate statistics, except the sample size
        if ( !metric.isInGroup( MetricGroup.UNIVARIATE_STATISTIC ) || metric == MetricConstants.SAMPLE_SIZE )
        {
            // Use the left name. Should probably use the triple of variable names for accuracy. See #81790.
            variableName = evaluation.getLeftVariableName();
        }
        else if ( Objects.nonNull( component ) )
        {
            // Get the name that corresponds to the side of the component. Again, should probably use the triple.
            switch ( component )
            {
                case LEFT -> variableName = evaluation.getLeftVariableName();
                case RIGHT -> variableName = evaluation.getRightVariableName();
                case BASELINE -> variableName = evaluation.getBaselineVariableName();
                default ->
                {
                    // Do nothing
                }
            }
        }

        return variableName;
    }

    /**
     * Returns the time scale of the evaluation for the plot title.
     *
     * @param metadata the statistics metadata
     * @param durationUnits the lead duration units
     * @return the time scale
     * @throws NullPointerException if either input is null
     */

    private String getTimeScaleForTitle( PoolMetadata metadata, ChronoUnit durationUnits )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( durationUnits );

        String timeScale = "";
        if ( metadata.hasTimeScale() )
        {
            // Use the default string representation of an instantaneous time scale
            // See #62867
            if ( metadata.getTimeScale().isInstantaneous() )
            {
                timeScale = metadata.getTimeScale().toString();
            }
            else
            {
                StringJoiner joiner = new StringJoiner( ", ", "[", "]" );

                TimeScaleOuter timeScaleOuter = metadata.getTimeScale();

                if ( timeScaleOuter.hasPeriod() )
                {
                    Number periodNumber = DataUtilities.durationToNumericUnits( metadata.getTimeScale()
                                                                                        .getPeriod(),
                                                                                durationUnits );
                    String period = periodNumber
                                    + " "
                                    + durationUnits.name().toUpperCase();

                    joiner.add( period );
                }

                joiner.add( timeScaleOuter.getFunction().toString() );

                MonthDay startMonthDay = timeScaleOuter.getStartMonthDay();

                if ( Objects.nonNull( startMonthDay ) )
                {
                    joiner.add( startMonthDay.toString() );
                }

                MonthDay endMonthDay = timeScaleOuter.getEndMonthDay();

                if ( Objects.nonNull( endMonthDay ) )
                {
                    joiner.add( endMonthDay.toString() );
                }

                timeScale = joiner.toString();
            }
        }

        return timeScale;
    }

    /**
     * Uncovers the threshold for the plot title.
     *
     * @param metadata the sample metadata
     * @param chartType the chart type
     * @param statisticType the type of statistic
     * @return the threshold
     * @throws NullPointerException if any input is null
     */

    private String getThresholdForTitle( PoolMetadata metadata,
                                         ChartType chartType,
                                         StatisticType statisticType )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( chartType );
        Objects.requireNonNull( statisticType );

        String threshold = "";

        if ( chartType == ChartType.THRESHOLD_LEAD && statisticType == StatisticType.DIAGRAM )
        {
            threshold = metadata.getThresholds()
                                .first()
                                .toString();
        }

        return threshold;
    }

    /**
     * Creates a baseline scenario name
     * @param metadata the output metadata
     * @param metric the metric
     * @return a baseline scenario name
     * @throws NullPointerException if either input is null
     */
    public String getBaselineScenarioForTitle( PoolMetadata metadata,
                                               MetricConstants metric )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( metric );

        String baselineScenario = "";

        // TODO: need a less brittle way to identify skill measures that have used a default baseline vs. an explicit 
        // one because a pool that includes an explicit baseline may or may not have been used for specific measures.
        if ( metric.isSkillMetric()
             && !metric.isInGroup( SampleDataGroup.DICHOTOMOUS )
             && metric != MetricConstants.KLING_GUPTA_EFFICIENCY )
        {

            String baselineSuffix = metadata.getEvaluation()
                                            .getBaselineDataName();

            // Skill scores for baseline use a default reference, which is climatology
            // This is also potentially brittle, so consider a better way, such as adding the default baseline
            // name into the evaluation description
            if ( metadata.getPool().getIsBaselinePool() )
            {
                baselineSuffix = metadata.getEvaluation()
                                         .getDefaultBaseline()
                                         .name()
                                         .replace( "_", " " );
            }

            baselineScenario = "against predictions from " + baselineSuffix;
        }

        return baselineScenario;
    }

    /**
     * Uncovers the georeferencing information for the plot title.
     *
     * @param metadata the sample metadata
     * @return the geographic name
     * @throws NullPointerException if the metadata is null
     */

    private String getGeoNameForTitle( PoolMetadata metadata )
    {
        Objects.requireNonNull( metadata );

        String regionName = metadata.getPool()
                                    .getGeometryGroup()
                                    .getRegionName();

        if ( regionName.isBlank() )
        {
            throw new IllegalArgumentException( "Failed to create parameters for graphics generation: the region "
                                                + "name was missing from the pool metadata, which is not allowed." );
        }

        return regionName;
    }

    /**
     * Uncovers the time window for the plot title.
     *
     * @param metadata the sample metadata
     * @param chartType the chart type
     * @param statisticType the type of statistic
     * @param durationUnits the duration units
     * @return the time window
     * @throws NullPointerException if the metadata is null
     */

    private String getTimeWindowForTitle( PoolMetadata metadata,
                                          ChartType chartType,
                                          StatisticType statisticType,
                                          ChronoUnit durationUnits )
    {
        Objects.requireNonNull( metadata );

        // No time window for pooling window plots unless they are diagrams and hence one diagram per time window
        if ( chartType == ChartType.POOLING_WINDOW
             && ( statisticType == StatisticType.DOUBLE_SCORE || statisticType == StatisticType.DURATION_SCORE ) )
        {
            return "";
        }

        TimeWindowOuter timeWindow = metadata.getTimeWindow();

        String timeWindowString = "";

        boolean addedOne = false;

        if ( !timeWindow.hasUnboundedReferenceTimes() )
        {
            timeWindowString += "reference times in ["
                                + timeWindow.getEarliestReferenceTime()
                                + ", "
                                + timeWindow.getLatestReferenceTime()
                                + "]";
            addedOne = true;
        }

        if ( !timeWindow.hasUnboundedValidTimes() )
        {
            String start = "";
            if ( addedOne )
            {
                start = " and ";
            }

            timeWindowString += start + "valid times in ["
                                + timeWindow.getEarliestValidTime()
                                + ", "
                                + timeWindow.getLatestValidTime()
                                + "]";
        }

        // For diagrams, add the lead duration if the format is by lead duration
        if ( ChartFactory.DIAGRAMS_WITHOUT_LEAD_AXIS.contains( statisticType )
             && !timeWindow.bothLeadDurationsAreUnbounded() )
        {
            String start = "";
            if ( addedOne )
            {
                start = " and ";
            }

            String middle;

            Number earliestNumber = DataUtilities.durationToNumericUnits( timeWindow.getEarliestLeadDuration(),
                                                                          durationUnits );

            if ( timeWindow.getEarliestLeadDuration()
                           .equals( timeWindow.getLatestLeadDuration() ) )
            {
                middle = "a lead duration of " + earliestNumber + " ";
            }
            else
            {
                Number latestNumber = DataUtilities.durationToNumericUnits( timeWindow.getLatestLeadDuration(),
                                                                            durationUnits );

                middle = "lead durations in ["
                         + earliestNumber
                         + ", "
                         + latestNumber
                         + "] ";
            }

            timeWindowString += start + middle
                                + durationUnits.toString().toUpperCase();
        }

        return timeWindowString;
    }

    /**
     * Returns a legend name from the inputs.
     * @param metricName the metric name
     * @param metadata the pool metadata
     * @param chartType the chart type
     * @param graphicShape the graphic shape
     * @param durationUnits the duration units
     * @throws NullPointerException if any input is null
     */
    private String getLegendName( MetricConstants metricName,
                                  PoolMetadata metadata,
                                  ChartType chartType,
                                  GraphicShape graphicShape,
                                  ChronoUnit durationUnits )
    {
        Objects.requireNonNull( metricName );
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( chartType );
        Objects.requireNonNull( graphicShape );
        Objects.requireNonNull( durationUnits );

        String legendTitle = "";
        String thresholdUnits = metadata.getEvaluation()
                                        .getMeasurementUnit();

        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        // Special case
        // TODO: eliminate this special case and handle more generally
        if ( metricName == MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM )
        {
            legendTitle = "Name: ";
        }
        // Lead-threshold
        else if ( chartType == ChartType.LEAD_THRESHOLD || chartType == ChartType.TIMING_ERROR_SUMMARY_STATISTICS )
        {
            legendTitle = "Threshold [" + thresholdUnits + "]:";
        }
        // Threshold-lead
        else if ( chartType == ChartType.THRESHOLD_LEAD )
        {
            legendTitle = "Latest lead time [" + leadUnits + "]:";
        }
        // Pooling windows
        else if ( chartType == ChartType.POOLING_WINDOW )
        {
            legendTitle = this.getPoolingWindowLegendName( metricName, metadata, graphicShape, durationUnits );
        }

        return legendTitle;
    }

    /**
     * Returns a legend name from the inputs for a chart that displays statistics for each of several pools.
     * @param metricName the metric name
     * @param metadata the pool metadata
     * @param graphicShape the graphic shape
     * @param durationUnits the duration units
     * @throws NullPointerException if any input is null
     */
    private String getPoolingWindowLegendName( MetricConstants metricName,
                                               PoolMetadata metadata,
                                               GraphicShape graphicShape,
                                               ChronoUnit durationUnits )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( graphicShape );
        Objects.requireNonNull( durationUnits );

        String legendTitle = "";
        String thresholdUnits = metadata.getEvaluation()
                                        .getMeasurementUnit();

        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        Duration earliest = metadata.getTimeWindow()
                                    .getEarliestLeadDuration();
        Duration latest = metadata.getTimeWindow()
                                  .getLatestLeadDuration();
        Instant earliestValidTime = metadata.getTimeWindow()
                                            .getEarliestValidTime();
        Instant latestValidTime = metadata.getTimeWindow()
                                          .getLatestValidTime();
        Instant earliestReferenceTime = metadata.getTimeWindow()
                                                .getEarliestReferenceTime();
        Instant latestReferenceTime = metadata.getTimeWindow()
                                              .getLatestReferenceTime();

        // Plots for scores contain all pools, so qualify the time components
        if ( metricName.isInGroup( StatisticType.DOUBLE_SCORE )
             || metricName.isInGroup( StatisticType.DURATION_SCORE ) )
        {
            // Lead durations where required
            if ( !earliest.equals( TimeWindowOuter.DURATION_MIN ) || !latest.equals( TimeWindowOuter.DURATION_MAX ) )
            {
                legendTitle = legendTitle + "Lead time window [" + leadUnits + "], ";
            }

            // Valid times where required
            if ( graphicShape != GraphicShape.VALID_DATE_POOLS
                 && ( !earliestValidTime.equals( Instant.MIN ) || !latestValidTime.equals( Instant.MAX ) ) )
            {
                legendTitle = legendTitle + "Valid time window [UTC], ";
            }

            // Reference times where required
            if ( graphicShape != GraphicShape.ISSUED_DATE_POOLS
                 && ( !earliestReferenceTime.equals( Instant.MIN ) || !latestReferenceTime.equals( Instant.MAX ) ) )
            {
                legendTitle = legendTitle + "Issued time window [UTC], ";
            }
        }

        legendTitle = legendTitle + "Threshold [" + thresholdUnits + "]:";

        return legendTitle;
    }

    /**
     * @param metricName the metric name.
     * @param graphicShape The graphic shape.
     * @return the chart type for the plot.
     * @throws NullPointerException if any input is null
     */
    private static ChartType getChartType( MetricConstants metricName,
                                           GraphicShape graphicShape )
    {
        Objects.requireNonNull( graphicShape );
        Objects.requireNonNull( metricName );

        // Pooling window case
        if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS || graphicShape == GraphicShape.VALID_DATE_POOLS )
        {
            return ChartType.POOLING_WINDOW;
        }

        // Default shape
        if ( graphicShape == GraphicShape.DEFAULT )
        {
            StatisticType statisticType = metricName.getMetricOutputGroup();
            return ChartFactory.getChartType( statisticType );
        }

        String name = graphicShape.name();
        return ChartType.valueOf( name );
    }

    /**
     * Translates a statistic type into a chart type.
     * @param statisticType the statistic type
     * @return the chart type
     * @throws NullPointerException if the statisticType is null
     * @throws IllegalArgumentException if the statisticType is not recognized
     */

    private static ChartType getChartType( StatisticType statisticType )
    {
        Objects.requireNonNull( statisticType );

        return switch ( statisticType )
                {
                    case BOXPLOT_PER_PAIR, BOXPLOT_PER_POOL, DURATION_DIAGRAM -> ChartType.UNIQUE;
                    case DIAGRAM, DOUBLE_SCORE, DURATION_SCORE -> ChartType.LEAD_THRESHOLD;
                };
    }

    /**
     * Creates the keys by which to slice diagram statistics. Each slice produces one diagram.
     *
     * @param statistics the statistics to slice
     * @param chartType the chart type
     * @return the keys for slicing
     */

    private Set<Object> getKeysForSlicingDiagrams( List<DiagramStatisticOuter> statistics, ChartType chartType )
    {
        Set<Object> keySetValues =
                Slicer.discover( statistics,
                                 next -> next.getPoolMetadata()
                                             .getTimeWindow() );

        // Slice by threshold if not time
        if ( chartType != ChartType.LEAD_THRESHOLD && chartType != ChartType.POOLING_WINDOW )
        {
            keySetValues = Slicer.discover( statistics,
                                            next -> next.getPoolMetadata()
                                                        .getThresholds() );
        }

        return keySetValues;
    }

    /**
     * @return the chart font
     */

    private Font getChartFont()
    {
        return this.chartFont;
    }

    /**
     * @return the chart font in bold type
     */

    private Font getChartTitleFont()
    {
        return this.chartFontTitle;
    }

    /**
     * @return the series colors
     */

    private Color[] getSeriesColors()
    {
        return this.seriesColors;
    }

    /**
     * @return a renderer
     */

    private BoxplotRenderer getBoxPlotRenderer()
    {
        // Do not cache (e.g., render static) this renderer because it retains references to the datasets it renders
        // This seems like a bug in the JFreeChart rendering design
        BoxplotRenderer boxplotRenderer = new BoxplotRenderer();
        Stroke stroke = new BasicStroke( 0.5f );
        boxplotRenderer.setDefaultStroke( stroke );
        boxplotRenderer.setDefaultFillPaint( Color.GREEN );
        boxplotRenderer.setDefaultPaint( Color.RED );

        return boxplotRenderer;
    }

    /**
     * Hidden constructor.
     */

    private ChartFactory()
    {
        this.seriesColors = GraphicsUtils.getColors();

        // #81628
        String fontResource = "LiberationSans-Regular.ttf";
        LOGGER.debug( "Attempting to find resource {} on the classpath.", fontResource );
        URL fontUrl = ChartFactory.class.getClassLoader()
                                        .getResource( fontResource );

        LOGGER.debug( "The URL for the font '{}' is: {}.", fontResource, fontUrl );

        Font chartFontInner = DEFAULT_CHART_FONT;
        Font chartFontTitleInner = DEFAULT_CHART_TITLE_FONT;

        // Load the font and force it into the chart.
        if ( Objects.isNull( fontUrl ) )
        {
            LOGGER.warn( "Failed to load font resource {} from the classpath. Using the default font '{}' for all "
                         + "graphics.", fontResource, DEFAULT_FONT_NAME );
        }
        else
        {
            try
            {
                LOGGER.debug( "Registering font {}...", fontResource );
                // Registering a font can apparently lead the JVM to "hang" momentarily if the OS cannot service the request
                // See #111762 for an example

                // Create from file, not stream
                // https://stackoverflow.com/questions/38783010/huge-amount-of-jf-tmp-files-in-var-cache-tomcat7-temp
                File fontFile = new File( fontUrl.toURI() );
                chartFontInner = Font.createFont( Font.TRUETYPE_FONT, fontFile )
                                     .deriveFont( 10.0f );

                File fontFileTitle = new File( fontUrl.toURI() );
                chartFontTitleInner = Font.createFont( Font.TRUETYPE_FONT, fontFileTitle )
                                          .deriveFont( 11.0f );

                // Register font with graphics env
                GraphicsEnvironment graphics = GraphicsEnvironment.getLocalGraphicsEnvironment();

                graphics.registerFont( chartFontInner );
                graphics.registerFont( chartFontTitleInner );

                LOGGER.debug( "Finished registering font {}.", fontResource );
            }
            catch ( URISyntaxException | FontFormatException | IOException e )
            {
                LOGGER.warn( "Failed to read font resource {} from {}. Using the default font '{}' for all "
                             + "graphics.", fontResource, fontUrl, DEFAULT_FONT_NAME );
            }
        }

        // Set the fonts
        this.chartFont = chartFontInner;
        this.chartFontTitle = chartFontTitleInner;
    }

}

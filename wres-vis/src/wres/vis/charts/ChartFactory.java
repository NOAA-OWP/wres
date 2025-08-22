package wres.vis.charts;

import java.awt.*;
import java.awt.geom.Ellipse2D;
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
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

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
import org.jfree.chart.renderer.xy.StandardXYBarPainter;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.chart.renderer.xy.XYErrorRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.chart.title.TextTitle;
import org.jfree.chart.ui.RectangleInsets;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.vis.charts.GraphicsUtils.BASELINE_SCENARIO_LABEL;
import static wres.vis.charts.GraphicsUtils.PAIR_THEME_LABEL_GENERATOR;
import static wres.vis.charts.GraphicsUtils.PAIR_THEME_SEPARATOR;
import static wres.vis.charts.GraphicsUtils.PREDICTED_SCENARIO_LABEL;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.TimeWindowSlicer;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.Covariate;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentType;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.PairsMetric;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.TimeWindow;
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
    /** The chart type. */
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

    /** Character limit for qualifying covariates. */
    private static final int COVARIATE_CHARACTER_LIMIT = 53;

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

    /** Re-used string. */
    private static final String AND = " and ";

    /** Re-used string. */
    private static final String OF_THE = " of the ";

    /** Date-time format string. */
    private static final String YYYY_MM_DD_HH = "yyyy-MM-dd+HH";

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

        // Use the metadata from the first element, plus the aggregate time window
        DurationScoreStatisticOuter example = statistics.get( 0 );
        PoolMetadata exampleMetadata = example.getPoolMetadata();
        Set<PoolMetadata> metadatas = statistics.stream()
                                                .map( DurationScoreStatisticOuter::getPoolMetadata )
                                                .collect( Collectors.toSet() );

        CategoryDataset source = ChartDataFactory.ofDurationScoreSummaryStatistics( statistics );
        MetricConstants metricName = example.getMetricName();

        SummaryStatistic summaryStatistic = example.getSummaryStatistic();

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        Set<TimeWindowOuter> timeWindows = statistics.stream()
                                                     .map( n -> n.getPoolMetadata()
                                                                 .getTimeWindow() )
                                                     .collect( Collectors.toSet() );
        TimeWindowOuter aggregateTimeWindow = this.getAggregateTimeWindow( timeWindows );

        String legendTitle = this.getLegendName( metricName,
                                                 ChartType.TIMING_ERROR_SUMMARY_STATISTICS,
                                                 GraphicShape.DEFAULT,
                                                 durationUnits,
                                                 timeWindows,
                                                 exampleMetadata.getEvaluation()
                                                                .getMeasurementUnit() );

        ChartTitleParameters parameters = new ChartTitleParameters( metadatas,
                                                                    aggregateTimeWindow,
                                                                    Pair.of( metricName, null ),
                                                                    durationUnits,
                                                                    ChartType.TIMING_ERROR_SUMMARY_STATISTICS,
                                                                    StatisticType.DURATION_SCORE,
                                                                    null,
                                                                    quantiles,
                                                                    summaryStatistic,
                                                                    null );
        String title = this.getChartTitle( parameters );

        String rangeTitle = metricName.toString() + " [HOURS]";

        // Summary statistic across a non-resampled dimension?
        if ( Objects.nonNull( summaryStatistic )
             && !summaryStatistic.getDimensionList()
                                 .contains( SummaryStatistic.StatisticDimension.RESAMPLED ) )
        {
            rangeTitle = summaryStatistic.getStatistic()
                                         .name()
                                         .replace( "_", " " )
                         + OF_THE
                         + rangeTitle;
        }

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

        // Eliminate duplicate legend items, which may occur when there are multiple dataset orientations. Since all
        // datasets are plotted with the same color, only a different shade, eliminate the duplicate entries
        LegendItemCollection existing = plot.getLegendItems();
        LegendItemCollection adjusted = new LegendItemCollection();
        int count = existing.getItemCount();
        for ( int i = 0; i < count; i++ )
        {
            LegendItem next = existing.get( i );
            if ( !next.getLabel()
                      .endsWith( "baseline" ) )
            {
                adjusted.add( next );
            }
        }

        plot.setFixedLegendItems( adjusted );

        chart.setAntiAlias( true );
        this.setChartPadding( chart );
        this.setChartTheme( chart );

        int rowCount = plot.getDataset()
                           .getRowCount();

        // Two dataset orientations together? If so, wash/brighten colors for second/baseline
        boolean washAlternate = statistics.stream()
                                          .anyMatch( s -> s.getPoolMetadata()
                                                           .getPoolDescription()
                                                           .getIsBaselinePool() )
                                && statistics.stream()
                                             .anyMatch( s -> !s.getPoolMetadata()
                                                               .getPoolDescription()
                                                               .getIsBaselinePool() );

        BarRenderer renderer = this.getDurationScoreSeriesRenderer( rowCount, washAlternate );
        plot.setRenderer( renderer );

        // Set the legend on/off and the title
        if ( this.setLegendVisible( chart, true ) )
        {
            this.setLegendTitle( chart, legendTitle, true );
        }

        this.setCategoryPlotAxes( chart.getCategoryPlot() );

        return chart;
    }

    /**
     * Returns a collection of diagrams.
     * @param statistics the statistics
     * @param graphicShape the shape of the graphic to plot
     * @param durationUnits the duration units
     * @return a map of diagrams by data slice key
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    public Map<Object, JFreeChart> getDiagramCharts( List<DiagramStatisticOuter> statistics,
                                                     GraphicShape graphicShape,
                                                     ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

        ConcurrentMap<Object, JFreeChart> results = new ConcurrentSkipListMap<>();

        // Use the metadata from the first element
        DiagramStatisticOuter first = statistics.get( 0 );
        PoolMetadata exampleMetadata = first.getPoolMetadata();

        MetricConstants metricName = first.getMetricName();
        DiagramMetricComponent domain = first.getComponent( DiagramComponentType.PRIMARY_DOMAIN_AXIS );
        String summaryStatisticNameQualifier = this.getDiagramStatisticNameQualifier( first );

        DiagramMetricComponent range = first.getComponent( DiagramComponentType.PRIMARY_RANGE_AXIS );
        MetricDimension domainDimension = first.getComponentName( DiagramComponentType.PRIMARY_DOMAIN_AXIS );
        MetricDimension rangeDimension = first.getComponentName( DiagramComponentType.PRIMARY_RANGE_AXIS );
        boolean hasDiagonal = first.getStatistic()
                                   .getMetric()
                                   .getHasDiagonal();

        EnsembleAverageType ensembleAverageType = exampleMetadata.getPoolDescription()
                                                                 .getEnsembleAverageType();
        SummaryStatistic summaryStatistic = first.getSummaryStatistic();

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
        String legendTitle = this.getLegendName( metricName,
                                                 chartType,
                                                 graphicShape,
                                                 durationUnits,
                                                 Set.of(),
                                                 exampleMetadata.getEvaluation()
                                                                .getMeasurementUnit() );

        // One chart per key instance
        for ( Object keyInstance : keySetValues )
        {
            // Slice the statistics for the diagram
            List<DiagramStatisticOuter> slicedStatistics =
                    ChartDataFactory.getSlicedStatisticsForDiagram( keyInstance,
                                                                    statistics,
                                                                    chartType );

            SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

            Set<PoolMetadata> metadatas = slicedStatistics.stream()
                                                          .map( DiagramStatisticOuter::getPoolMetadata )
                                                          .collect( Collectors.toSet() );
            // Get an example metadata for the time window
            PoolMetadata timeWindowMetadata = metadatas.iterator()
                                                       .next();

            ChartTitleParameters parameters = new ChartTitleParameters( metadatas,
                                                                        timeWindowMetadata.getTimeWindow(),
                                                                        Pair.of( metricName, null ),
                                                                        durationUnits,
                                                                        chartType,
                                                                        StatisticType.DIAGRAM,
                                                                        ensembleAverageType,
                                                                        quantiles,
                                                                        summaryStatistic,
                                                                        summaryStatisticNameQualifier );

            String title = this.getChartTitle( parameters );

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
            else if ( metricName == MetricConstants.HISTOGRAM )
            {
                IntervalXYDataset dataset = ( IntervalXYDataset ) this.getDiagramDataset( chartType,
                                                                                          slicedStatistics,
                                                                                          domainDimension,
                                                                                          rangeDimension,
                                                                                          durationUnits );

                chart = org.jfree.chart.ChartFactory.createXYBarChart( title,
                                                                       domainTitle,
                                                                       false,
                                                                       rangeTitle,
                                                                       dataset );

                // Set the background paint
                XYPlot plot = chart.getXYPlot();
                plot.setBackgroundPaint( Color.WHITE );

                // Set the chart theme
                this.setChartTheme( chart );

                // Set the series renderer
                XYItemRenderer renderer = this.getSeriesColorAndShape( plot, metricName, !quantiles.isEmpty(), true );
                plot.setRenderer( renderer );

                // Set the axis offsets to zero. Could abstract to setting chart theme above, but breaks test benchmarks
                // for graphics scenarios
                plot.setAxisOffset( RectangleInsets.ZERO_INSETS );
            }
            else
            {
                XYDataset dataset = this.getDiagramDataset( chartType,
                                                            slicedStatistics,
                                                            domainDimension,
                                                            rangeDimension,
                                                            durationUnits );

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

                // Set the chart theme
                this.setChartTheme( chart );

                // Set the series renderer
                XYItemRenderer renderer = this.getSeriesColorAndShape( plot, metricName, !quantiles.isEmpty(), true );
                plot.setRenderer( renderer );
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

        if ( statistics.isEmpty() )
        {
            throw new ChartBuildingException( "Could not create the duration diagram charts as none were supplied with "
                                              + "valid statistics." );
        }

        DurationDiagramStatisticOuter example = statistics.get( 0 );

        PoolMetadata exampleMetadata = example.getPoolMetadata();
        Set<PoolMetadata> metadatas = statistics.stream()
                                                .map( DurationDiagramStatisticOuter::getPoolMetadata )
                                                .collect( Collectors.toSet() );

        SummaryStatistic summaryStatistic = example.getSummaryStatistic();

        // Component name
        MetricConstants metricName = example.getMetricName();

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, GraphicShape.DEFAULT );

        // Build the source.
        XYDataset source = ChartDataFactory.ofDurationDiagramStatistics( statistics );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        Set<TimeWindowOuter> timeWindows = statistics.stream()
                                                     .map( n -> n.getPoolMetadata()
                                                                 .getTimeWindow() )
                                                     .collect( Collectors.toSet() );
        TimeWindowOuter aggregateTimeWindow = this.getAggregateTimeWindow( timeWindows );

        String legendTitle = this.getLegendName( metricName,
                                                 chartType,
                                                 GraphicShape.DEFAULT,
                                                 durationUnits,
                                                 Set.of(),
                                                 exampleMetadata.getEvaluation()
                                                                .getMeasurementUnit() );

        ChartTitleParameters parameters = new ChartTitleParameters( metadatas,
                                                                    aggregateTimeWindow,
                                                                    Pair.of( metricName, null ),
                                                                    durationUnits,
                                                                    chartType,
                                                                    StatisticType.DURATION_DIAGRAM,
                                                                    null,
                                                                    quantiles,
                                                                    summaryStatistic,
                                                                    null );
        String title = this.getChartTitle( parameters );

        String rangeTitle = metricName.toString() + " [HOURS]";

        // Summary statistic across a non-resampled dimension?
        if ( Objects.nonNull( summaryStatistic )
             && !summaryStatistic.getDimensionList()
                                 .contains( SummaryStatistic.StatisticDimension.RESAMPLED ) )
        {
            rangeTitle = summaryStatistic.getStatistic()
                                         .name()
                                         .replace( "_", " " )
                         + OF_THE
                         + rangeTitle;
        }

        String domainTitle = this.getDurationDiagramDomainTitle( example );

        JFreeChart chart = org.jfree.chart.ChartFactory.createTimeSeriesChart( title,
                                                                               domainTitle,
                                                                               rangeTitle,
                                                                               source,
                                                                               true,
                                                                               false,
                                                                               false );

        // Set the date/time format. See GitHub ticket #360
        DateAxis dateAxis = ( DateAxis ) chart.getXYPlot()

                                              .getDomainAxis();
        TimeZone timeZone = TimeZone.getTimeZone( "UTC" );
        SimpleDateFormat format = new SimpleDateFormat( YYYY_MM_DD_HH );
        format.setTimeZone( timeZone );
        dateAxis.setDateFormatOverride( format );
        dateAxis.setTimeZone( timeZone );

        XYPlot plot = chart.getXYPlot();

        chart.setAntiAlias( true );
        this.setChartTheme( chart );
        this.setChartPadding( chart );

        // Set the series renderer
        XYItemRenderer renderer = this.getSeriesColorAndShape( plot, metricName, !quantiles.isEmpty(), true );
        plot.setRenderer( renderer );

        // Set the legend on/off and the title
        if ( this.setLegendVisible( chart, false ) )
        {
            this.setLegendTitle( chart, legendTitle, false );
        }

        this.setXYPlotAxes( chart.getXYPlot(), 0, 0, 0, 0, false, true ); // Auto-fit axes

        return chart;
    }

    /**
     * Creates a box plot chart containing data for several pools.
     *
     * @param statistics the metric output to plot
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    public JFreeChart getBoxplotChart( List<BoxplotStatisticOuter> statistics,
                                       ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

        // Use the metadata from the first element, plus the aggregate time window
        BoxplotStatisticOuter first = statistics.get( 0 );

        MetricConstants metricName = first.getMetricName();
        PoolMetadata exampleMetadata = first.getPoolMetadata();
        Set<PoolMetadata> metadatas = statistics.stream()
                                                .map( BoxplotStatisticOuter::getPoolMetadata )
                                                .collect( Collectors.toSet() );
        BoxplotMetric metric = first.getStatistic()
                                    .getMetric();
        QuantileValueType type = metric.getQuantileValueType();
        String metricUnits = metric.getUnits();
        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        EnsembleAverageType ensembleAverageType = exampleMetadata.getPoolDescription()
                                                                 .getEnsembleAverageType();

        SummaryStatistic summaryStatistic = first.getSummaryStatistic();

        String typeString = type.toString();

        String summaryStatisticNameQualifier = null;
        if ( Objects.nonNull( summaryStatistic ) )
        {
            summaryStatisticNameQualifier = metric.getStatisticName()
                                                  .toString()
                                                  .replace( "_", " " );

            if ( GraphicsUtils.isNotDefaultMetricComponentName( metric.getStatisticComponentName() ) )
            {
                summaryStatisticNameQualifier = metric.getStatisticComponentName()
                                                + OF_THE
                                                + summaryStatisticNameQualifier;
            }

            typeString = summaryStatisticNameQualifier;
        }

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, GraphicShape.DEFAULT );

        // Build the source
        XYDataset source = ChartDataFactory.ofBoxplotStatistics( statistics, durationUnits );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        Set<TimeWindowOuter> timeWindows = statistics.stream()
                                                     .map( n -> n.getPoolMetadata()
                                                                 .getTimeWindow() )
                                                     .collect( Collectors.toSet() );
        TimeWindowOuter aggregateTimeWindow = this.getAggregateTimeWindow( timeWindows );

        ChartTitleParameters parameters = new ChartTitleParameters( metadatas,
                                                                    aggregateTimeWindow,
                                                                    Pair.of( metricName, null ),
                                                                    durationUnits,
                                                                    chartType,
                                                                    metricName.getMetricOutputGroup(),
                                                                    ensembleAverageType,
                                                                    quantiles,
                                                                    summaryStatistic,
                                                                    summaryStatisticNameQualifier );
        String title = this.getChartTitle( parameters );

        String rangeTitle = typeString.replace( "_", " " )
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
        this.setXYPlotAxes( plot, 0, 0, 0, 0, false, true ); // Auto-fit axes

        return chart;
    }

    /**
     * Creates a box plot chart for each pool.
     *
     * @param statistics the metric output to plot
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance for each pool
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    public Map<TimeWindowOuter, JFreeChart> getBoxplotChartPerPool( List<BoxplotStatisticOuter> statistics,
                                                                    ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

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
     * Creates a pairs chart.
     *
     * @param statistics the metric output to plot
     * @param durationUnits the duration units
     * @return a {@link JFreeChart} instance for each pool
     * @throws NullPointerException if any input is null
     * @throws ChartBuildingException if the chart fails to build for any other reason
     */
    public JFreeChart getPairsChart( PairsStatisticOuter statistics,
                                     ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

        MetricConstants metricName = statistics.getMetricName();
        PairsMetric metric = statistics.getStatistic()
                                       .getMetric();
        String valueUnits = metric.getUnits();
        PoolMetadata metadata = statistics.getPoolMetadata();

        EnsembleAverageType ensembleAverageType = metadata.getPoolDescription()
                                                          .getEnsembleAverageType();

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, GraphicShape.DEFAULT );

        // Build the dataset
        XYDataset source = ChartDataFactory.ofPairsStatistics( statistics );

        int seriesCount = source.getSeriesCount();
        int maxPairs = 0;
        for ( int i = 0; i < seriesCount; i++ )
        {
            maxPairs = Math.max( source.getItemCount( i ), maxPairs );
        }

        TimeWindowOuter timeWindow = metadata.getTimeWindow();
        ChartTitleParameters parameters = new ChartTitleParameters( Set.of( metadata ),
                                                                    timeWindow,
                                                                    Pair.of( metricName, null ),
                                                                    durationUnits,
                                                                    chartType,
                                                                    metricName.getMetricOutputGroup(),
                                                                    ensembleAverageType,
                                                                    Collections.emptySortedSet(),
                                                                    null,
                                                                    null );
        String title = this.getChartTitle( parameters );

        // Create the titles
        String domainTitle = "Time [UTC]";

        String variableName = this.getVariableName( metadata, statistics.getMetricName(), null );

        if ( variableName.isEmpty() )
        {
            variableName = "Value";
        }

        String rangeTitle = variableName
                            + " ["
                            + valueUnits
                            + "]";

        JFreeChart chart = org.jfree.chart.ChartFactory.createTimeSeriesChart( title,
                                                                               domainTitle,
                                                                               rangeTitle,
                                                                               source,
                                                                               true,
                                                                               false,
                                                                               false );

        DateAxis dateAxis = ( DateAxis ) chart.getXYPlot()
                                              .getDomainAxis();
        TimeZone timeZone = TimeZone.getTimeZone( "UTC" );
        SimpleDateFormat format = new SimpleDateFormat( YYYY_MM_DD_HH );
        format.setTimeZone( timeZone );
        dateAxis.setDateFormatOverride( format );
        dateAxis.setTimeZone( timeZone );

        XYPlot plot = chart.getXYPlot();

        chart.setAntiAlias( true );
        this.setChartTheme( chart );
        this.setChartPadding( chart );

        // Set the series renderer
        XYItemRenderer renderer = this.getSeriesColorAndShape( plot,
                                                               metricName,
                                                               false,
                                                               maxPairs <= 1 );
        plot.setRenderer( renderer );

        // To quote the documentation, this setting "usually" improve the appearance of charts. However, experimentation
        // indicates that it reduces the quality of the box plots
        chart.setAntiAlias( false );

        this.setXYPlotAxes( chart.getXYPlot(),
                            0,
                            0,
                            0,
                            0,
                            false,
                            true ); // Auto-fit axes

        LOGGER.debug( "Created a pairs plot for metric {}.", metricName );

        return chart;
    }

    /**
     * Generate a diagram dataset.
     * @param chartType the chart type
     * @param slicedStatistics the sliced statistics
     * @param domainDimension the domain axis dimension
     * @param rangeDimension the range axis dimension
     * @param durationUnits the duration units
     * @return the diagram dataset
     */
    private XYDataset getDiagramDataset( ChartType chartType,
                                         List<DiagramStatisticOuter> slicedStatistics,
                                         MetricDimension domainDimension,
                                         MetricDimension rangeDimension,
                                         ChronoUnit durationUnits )
    {
        XYDataset dataset;

        // One lead duration and up to many thresholds
        if ( chartType == ChartType.LEAD_THRESHOLD
             || chartType == ChartType.POOLING_WINDOW )
        {
            dataset = ChartDataFactory.ofDiagramStatisticsByLeadAndThreshold( slicedStatistics,
                                                                              domainDimension,
                                                                              rangeDimension,
                                                                              durationUnits );
        }
        // One threshold and up to many lead durations
        else
        {
            dataset = ChartDataFactory.ofDiagramStatisticsByThresholdAndLead( slicedStatistics,
                                                                              domainDimension,
                                                                              rangeDimension,
                                                                              durationUnits );
        }

        return dataset;
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

        // Use the metadata from the first element, plus the aggregate time window
        BoxplotStatisticOuter first = statistics.get( 0 );

        MetricConstants metricName = first.getMetricName();
        PoolMetadata exampleMetadata = first.getPoolMetadata();
        Set<PoolMetadata> metadatas = statistics.stream()
                                                .map( BoxplotStatisticOuter::getPoolMetadata )
                                                .collect( Collectors.toSet() );
        BoxplotMetric metric = first.getStatistic()
                                    .getMetric();
        QuantileValueType type = metric.getQuantileValueType();
        String metricUnits = metric.getUnits();

        EnsembleAverageType ensembleAverageType = exampleMetadata.getPoolDescription()
                                                                 .getEnsembleAverageType();

        SummaryStatistic summaryStatistic = first.getSummaryStatistic();

        // Determine the output type
        ChartType chartType = ChartFactory.getChartType( metricName, GraphicShape.DEFAULT );

        // Build the source
        XYDataset source = ChartDataFactory.ofBoxplotStatistics( statistics, durationUnits );

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        Set<TimeWindowOuter> timeWindows = statistics.stream()
                                                     .map( n -> n.getPoolMetadata()
                                                                 .getTimeWindow() )
                                                     .collect( Collectors.toSet() );
        TimeWindowOuter aggregateTimeWindow = this.getAggregateTimeWindow( timeWindows );
        ChartTitleParameters parameters = new ChartTitleParameters( metadatas,
                                                                    aggregateTimeWindow,
                                                                    Pair.of( metricName, null ),
                                                                    durationUnits,
                                                                    chartType,
                                                                    metricName.getMetricOutputGroup(),
                                                                    ensembleAverageType,
                                                                    quantiles,
                                                                    summaryStatistic,
                                                                    null );
        String title = this.getChartTitle( parameters );

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
        this.setXYPlotAxes( plot, 0, 0, 0, 0, false, true ); // Auto-fit axes

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
        // Use the metadata from the first element, plus the aggregate time window
        DoubleScoreComponentOuter example = statistics.get( 0 );
        PoolMetadata exampleMetadata = example.getPoolMetadata();
        Set<PoolMetadata> metadatas = statistics.stream()
                                                .map( DoubleScoreComponentOuter::getPoolMetadata )
                                                .collect( Collectors.toSet() );
        String metricUnits = example.getStatistic()
                                    .getMetric()
                                    .getUnits();

        SummaryStatistic summaryStatistic = example.getSummaryStatistic();

        // Component name
        MetricConstants metricComponentName = example.getMetricName();

        EnsembleAverageType ensembleAverageType = exampleMetadata.getPoolDescription()
                                                                 .getEnsembleAverageType();

        // Do not qualify a "main" score component because it is qualified by the overall metric name
        if ( metricComponentName == MetricConstants.MAIN )
        {
            metricComponentName = null;
        }

        // Determine the chart type
        ChartType chartType = ChartFactory.getChartType( metricName, graphicShape );

        String thresholdUnits = exampleMetadata.getEvaluation()
                                               .getMeasurementUnit();

        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        SortedSet<Double> quantiles = ChartDataFactory.getQuantiles( statistics );

        Set<TimeWindowOuter> timeWindows = statistics.stream()
                                                     .map( n -> n.getPoolMetadata()
                                                                 .getTimeWindow() )
                                                     .collect( Collectors.toSet() );
        TimeWindowOuter aggregateTimeWindow = this.getAggregateTimeWindow( timeWindows );

        String legendTitle = this.getLegendName( metricName,
                                                 chartType,
                                                 graphicShape,
                                                 durationUnits,
                                                 timeWindows,
                                                 thresholdUnits );

        ChartTitleParameters parameters = new ChartTitleParameters( metadatas,
                                                                    aggregateTimeWindow,
                                                                    Pair.of( metricName, metricComponentName ),
                                                                    durationUnits,
                                                                    chartType,
                                                                    StatisticType.DOUBLE_SCORE,
                                                                    ensembleAverageType,
                                                                    quantiles,
                                                                    summaryStatistic,
                                                                    null );
        String title = this.getChartTitle( parameters );
        String rangeTitle = metricName.toString()
                            + " ["
                            + metricUnits
                            + "]";

        // Summary statistic across a non-resampled dimension?
        if ( Objects.nonNull( summaryStatistic )
             && !summaryStatistic.getDimensionList()
                                 .contains( SummaryStatistic.StatisticDimension.RESAMPLED ) )
        {
            rangeTitle = summaryStatistic.getStatistic()
                                         .name()
                                         .replace( "_", " " )
                         + OF_THE
                         + rangeTitle;
        }

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
            // Set the date/time format. See GitHub ticket #360
            DateAxis dateAxis = ( DateAxis ) chart.getXYPlot()
                                                  .getDomainAxis();
            TimeZone timeZone = TimeZone.getTimeZone( "UTC" );
            SimpleDateFormat format = new SimpleDateFormat( YYYY_MM_DD_HH );
            format.setTimeZone( timeZone );
            dateAxis.setDateFormatOverride( format );
            dateAxis.setTimeZone( timeZone );
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

        // Set the series renderer
        XYItemRenderer renderer = this.getSeriesColorAndShape( plot,
                                                               metricName,
                                                               !quantiles.isEmpty(),
                                                               true );
        plot.setRenderer( renderer );

        // Set the legend on/off and the title
        if ( this.setLegendVisible( chart, false ) )
        {
            this.setLegendTitle( chart, legendTitle, false );
        }

        this.setXYPlotAxes( chart.getXYPlot(), 0, 0, 0, 0, false, true ); // Auto-fit axes

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
            sampleSize = ChartDataFactory.ofDiagramStatisticsByLeadAndThreshold( statistics,
                                                                                 MetricDimension.FORECAST_PROBABILITY,
                                                                                 MetricDimension.SAMPLE_SIZE,
                                                                                 durationUnits );
            reliability = ChartDataFactory.ofDiagramStatisticsByLeadAndThreshold( statistics,
                                                                                  MetricDimension.FORECAST_PROBABILITY,
                                                                                  MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                                  durationUnits );
        }
        else
        {
            sampleSize = ChartDataFactory.ofDiagramStatisticsByThresholdAndLead( statistics,
                                                                                 MetricDimension.FORECAST_PROBABILITY,
                                                                                 MetricDimension.SAMPLE_SIZE,
                                                                                 durationUnits );
            reliability = ChartDataFactory.ofDiagramStatisticsByThresholdAndLead( statistics,
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

        // Set the series renderer
        XYItemRenderer renderer = this.getSeriesColorAndShape( reliabilityPlot,
                                                               MetricConstants.RELIABILITY_DIAGRAM,
                                                               !quantiles.isEmpty(),
                                                               true );
        reliabilityPlot.setRenderer( renderer );

        XYPlot sampleSizePlot = new XYPlot( sampleSize, domainAxis, secondaryRangeAxis, null );
        this.setXYPlotAxes( sampleSizePlot, 0, 1, 0, 0, false, false );

        // The reliability plot controls the legend, so remove legend items from the sample size plot
        LegendItemCollection noLegendItems = new LegendItemCollection();
        sampleSizePlot.setFixedLegendItems( noLegendItems );

        // Set the series renderer
        XYItemRenderer sampleRenderer = this.getSeriesColorAndShape( sampleSizePlot,
                                                                     MetricConstants.RELIABILITY_DIAGRAM,
                                                                     !quantiles.isEmpty(),
                                                                     true );
        sampleSizePlot.setRenderer( sampleRenderer );

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
     * Gets the color and shape for each series depending on the parameters supplied
     *
     * @param plot the plot
     * @param metric the metric name
     * @param errorBars is true to plot error bars, false otherwise
     * @param showShapes is true to show shapes in a line and shape renderer, false for lines only
     * @return the renderer
     * @throws NullPointerException if the plot is null
     */

    private XYItemRenderer getSeriesColorAndShape( XYPlot plot,
                                                   MetricConstants metric,
                                                   boolean errorBars,
                                                   boolean showShapes )
    {
        XYItemRenderer renderer;
        if ( metric == MetricConstants.HISTOGRAM )
        {
            renderer = this.getBarRenderer( plot );
        }
        else if ( metric == MetricConstants.SCATTER_PLOT )
        {
            double size = 4.0;
            double delta = size / 2.0;
            Shape shape = new Ellipse2D.Double( -delta, -delta, size, size );
            renderer = this.getLineAndShapeRenderer( plot,
                                                     errorBars,
                                                     false,
                                                     showShapes,
                                                     false,
                                                     false,
                                                     new Shape[] { shape } );
        }
        // Omit shapes and do not dash duplicate series
        else if ( metric.isInGroup( StatisticType.PAIRS ) )
        {
            renderer = this.getLineAndShapeRenderer( plot,
                                                     errorBars,
                                                     true,
                                                     showShapes,
                                                     false,
                                                     metric == MetricConstants.SPAGHETTI_PLOT,
                                                     DefaultDrawingSupplier.DEFAULT_SHAPE_SEQUENCE );
        }
        else
        {
            renderer = this.getLineAndShapeRenderer( plot,
                                                     errorBars,
                                                     true,
                                                     showShapes,
                                                     true,
                                                     false,
                                                     DefaultDrawingSupplier.DEFAULT_SHAPE_SEQUENCE );
        }

        return renderer;
    }

    /**
     * Gets a line and shape renderer.
     *
     * @param plot the plot
     * @param errorBars is true to plot error bars, false otherwise
     * @param showLines is true to plot lines
     * @param showShapes is true to plot shapes
     * @param shapes the shape sequence to use
     * @param dashMultiSeries is true to use a dashed line for plots with multiple overlapping series
     * @param highlightObservations is true to highlight observed series in a time-series plot
     * @return the renderer
     * @throws NullPointerException if any nullable input is null
     */

    private XYItemRenderer getLineAndShapeRenderer( XYPlot plot,
                                                    boolean errorBars,
                                                    boolean showLines,
                                                    boolean showShapes,
                                                    boolean dashMultiSeries,
                                                    boolean highlightObservations,
                                                    Shape[] shapes )
    {
        Objects.requireNonNull( plot );
        Objects.requireNonNull( shapes );

        Color[] colors = this.getSeriesColors();

        // Determine the number of series and whether they are paired, as in multi-series plots
        Map<Integer, SortedSet<Integer>> seriesIndexes = this.getPairedSeriesIndexes( plot );

        // Too many series for the default color sequence? Generate a sequence instead
        int seriesCount = seriesIndexes.size();
        if ( colors.length < seriesCount )
        {
            colors = GraphicsUtils.getColorPalette( seriesCount, Color.BLUE, Color.GREEN, Color.RED );
        }

        XYLineAndShapeRenderer renderer;
        if ( errorBars )
        {
            XYErrorRenderer errors = new XYErrorRenderer();
            errors.setDrawXError( true );
            errors.setDrawYError( true );
            errors.setCapLength( ChartFactory.DEFAULT_ERROR_BAR_CAP_LENGTH );
            renderer = errors;
        }
        else
        {
            renderer = new XYLineAndShapeRenderer();
        }

        int colorShapeIndex = 0;
        for ( Map.Entry<Integer, SortedSet<Integer>> nextPair : seriesIndexes.entrySet() )
        {
            int leftIndex = nextPair.getKey();
            SortedSet<Integer> rightIndexes = nextPair.getValue();

            Pair<Color, Stroke> colorStroke = this.getSeriesColorAndStroke( colors[colorShapeIndex],
                                                                            renderer.getSeriesStroke( leftIndex ),
                                                                            plot.getDataset()
                                                                                .getSeriesKey( leftIndex )
                                                                                .toString(),
                                                                            highlightObservations );

            Color color = colorStroke.getLeft();
            Stroke stroke = colorStroke.getRight();

            renderer.setSeriesPaint( leftIndex, color );
            Shape shape = shapes[colorShapeIndex % shapes.length];
            renderer.setSeriesShape( leftIndex, shape );
            renderer.setSeriesShapesVisible( leftIndex, showShapes );
            renderer.setSeriesShapesFilled( leftIndex, true );
            renderer.setSeriesLinesVisible( leftIndex, showLines );
            renderer.setLegendItemLabelGenerator( PAIR_THEME_LABEL_GENERATOR );
            renderer.setSeriesStroke( leftIndex, stroke );

            // Paired series? Set this using the same color and shape
            if ( !Objects.equals( Set.of( leftIndex ), rightIndexes ) )
            {
                for ( int rightIndex : rightIndexes )
                {
                    renderer.setSeriesPaint( rightIndex, color );
                    renderer.setSeriesShape( rightIndex, shape );
                    renderer.setSeriesShapesVisible( rightIndex, showShapes );
                    renderer.setSeriesShapesFilled( rightIndex, true );
                    renderer.setSeriesLinesVisible( rightIndex, showLines );
                    renderer.setSeriesVisibleInLegend( rightIndex, false );

                    // Use a dashed line for the duplicate series in multi-series plots?
                    if ( dashMultiSeries )
                    {
                        renderer.setSeriesStroke( rightIndex,
                                                  new BasicStroke( 1.0f,
                                                                   BasicStroke.CAP_ROUND,
                                                                   BasicStroke.JOIN_ROUND,
                                                                   1.0f, new float[] { 6.0f, 6.0f }, 0.0f ) );
                    }
                    else
                    {
                        renderer.setSeriesStroke( rightIndex, stroke );
                    }
                }
            }

            colorShapeIndex++;
        }

        return renderer;
    }

    /**
     * Generates a series color and stroke from the inputs.
     * @param baseColor the base color
     * @param baseStroke the base stroke
     * @param seriesKey the series name
     * @param highlightObservations whether to highlight a series with a name that sounds like the left/observed data
     * @return the color and stroke
     */

    private Pair<Color, Stroke> getSeriesColorAndStroke( Color baseColor,
                                                         Stroke baseStroke,
                                                         String seriesKey,
                                                         boolean highlightObservations )
    {
        Color color = baseColor;
        Stroke stroke = baseStroke;

        if ( highlightObservations
             && seriesKey.toLowerCase()
                         .startsWith( DatasetOrientation.LEFT.toString() ) )
        {
            color = Color.BLACK;
            stroke = new BasicStroke( 1.5f );
        }

        return Pair.of( color, stroke );
    }

    /**
     * Returns the series index pairs for coordinating renderers across common datasets. If this is a multi-series
     * plot, the indexes within the pairs will differ and there could be multiple indexes on the right hand side,
     * otherwise they will be the same, i.e., no series whose rendering should be coordinated.
     *
     * @param plot the plot
     * @return the series indexes
     */

    private Map<Integer, SortedSet<Integer>> getPairedSeriesIndexes( XYPlot plot )
    {
        // Map the series indexes by key, accounting for series with duplicate names
        int seriesCount = plot.getSeriesCount();
        Map<String, SortedSet<Integer>> seriesByKey = new HashMap<>();
        for ( int i = 0; i < seriesCount; i++ )
        {
            String seriesName = plot.getDataset()
                                    .getSeriesKey( i )
                                    .toString();

            if ( seriesName.contains( PAIR_THEME_SEPARATOR ) )
            {
                seriesName = seriesName.substring( 0, seriesName.indexOf( PAIR_THEME_SEPARATOR ) );
            }

            if ( seriesByKey.containsKey( seriesName ) )
            {
                seriesByKey.get( seriesName )
                           .add( i );
            }
            else
            {
                SortedSet<Integer> container = new TreeSet<>();
                container.add( i );
                seriesByKey.put( seriesName,
                                 container );
            }
        }

        Map<Integer, SortedSet<Integer>> indexes = new HashMap<>();
        for ( Map.Entry<String, SortedSet<Integer>> nextEntry : seriesByKey.entrySet() )
        {
            String key = nextEntry.getKey();
            SortedSet<Integer> existing = nextEntry.getValue();

            // For plots that merge a predicted and a baseline scenario into one plot, ensure that the first (predicted)
            // key points to the baseline key for merging. In all other cases, retain the existing pairings created
            // above
            if ( !key.endsWith( BASELINE_SCENARIO_LABEL ) )
            {
                String paired = key + BASELINE_SCENARIO_LABEL;
                Set<Integer> dashedKeys = seriesByKey.getOrDefault( paired, existing );
                SortedSet<Integer> combined = new TreeSet<>( existing );
                combined.addAll( dashedKeys );
                combined.remove( existing.first() );
                indexes.put( existing.first(), combined );
            }
        }

        return Collections.unmodifiableMap( indexes );
    }

    /**
     * Gets a bar renderer.
     *
     * @param plot the plot
     * @return the renderer
     * @throws NullPointerException if the plot is null
     */

    private XYItemRenderer getBarRenderer( XYPlot plot )
    {
        Objects.requireNonNull( plot );

        Color[] colors = this.getSeriesColors();

        // Too many series for the default color sequence? Generate a sequence instead
        int seriesCount = plot.getSeriesCount();
        if ( colors.length < seriesCount )
        {
            colors = GraphicsUtils.getColorPalette( seriesCount, Color.BLUE, Color.GREEN, Color.RED );
        }

        // Set flat/default renderer
        XYBarRenderer.setDefaultBarPainter( new StandardXYBarPainter() );
        XYBarRenderer renderer = new XYBarRenderer( 0.2 );
        renderer.setShadowVisible( false );
        renderer.setDrawBarOutline( false );

        for ( int i = 0; i < seriesCount; i++ )
        {
            renderer.setSeriesPaint( i, colors[i] );
        }

        return renderer;
    }

    /**
     * Returns the renderer for a duration score series.
     *
     * @param seriesCount the series count
     * @param washAlternate whether to wash colors for alternate row keys
     * @return the renderer
     * @throws NullPointerException if the plot is null
     */

    private BarRenderer getDurationScoreSeriesRenderer( int seriesCount, boolean washAlternate )
    {
        BarRenderer renderer = new BarRenderer();

        // Solid bar colors
        renderer.setBarPainter( new StandardBarPainter() );

        // No padding between bars
        renderer.setItemMargin( 0 );

        // No shadow
        renderer.setShadowVisible( false );

        Color[] colors = this.getDurationScoreSeriesColors( seriesCount, washAlternate );

        for ( int i = 0; i < seriesCount; i++ )
        {
            renderer.setSeriesPaint( i, colors[i] );
        }

        return renderer;
    }

    /**
     * Generates a color pallete for a duration score chart.
     * @param seriesCount the number of series
     * @param washAlternate whether every other series should have the same color as the prior series, only brightened
     * @return the colors
     */

    private Color[] getDurationScoreSeriesColors( int seriesCount, boolean washAlternate )
    {
        int actualSeriesCount = seriesCount;
        Color[] colors = this.getSeriesColors();
        if ( washAlternate )
        {
            actualSeriesCount = seriesCount / 2;
        }

        // Too many series for the default color sequence? Generate a sequence instead
        if ( colors.length < actualSeriesCount )
        {
            colors = GraphicsUtils.getColorPalette( actualSeriesCount, Color.BLUE, Color.GREEN, Color.RED );
        }

        Color[] returnMe = new Color[seriesCount];

        if ( washAlternate )
        {
            for ( int i = 0; i < actualSeriesCount; i += 1 )
            {
                // Re-use colors but brighten the second series x2
                returnMe[i * 2] = colors[i];
                returnMe[( i * 2 ) + 1] = colors[i].brighter()
                                                   .brighter();
            }
        }
        else
        {
            System.arraycopy( colors, 0, returnMe, 0, seriesCount );
        }

        return returnMe;
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
                          chart.getTitle()
                               .getText(),
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
     * @param parameters the chart title parameters
     * @return the chart title
     * @throws NullPointerException if any required input is null
     */

    private String getChartTitle( ChartTitleParameters parameters )
    {
        SummaryStatistic summaryStatistic = parameters.summaryStatistic();

        Pair<MetricConstants, MetricConstants> metricNames = parameters.metricNames();
        MetricConstants metricName = metricNames.getLeft();
        String name = this.getSummaryStatisticQualifier( metricName, summaryStatistic );

        boolean isSummaryStatistic = Objects.nonNull( summaryStatistic )
                                     && !summaryStatistic.getDimensionList()
                                                         .contains( SummaryStatistic.StatisticDimension.RESAMPLED );

        String metric = metricName.toString();

        if ( isSummaryStatistic
             && Objects.nonNull( parameters.summaryStatisticNameQualifier() )
             && !parameters.summaryStatisticNameQualifier()
                           .isBlank() )
        {
            metric = parameters.summaryStatisticNameQualifier();
        }

        // Summary statistic that needs to be qualified
        name += metric;

        MetricConstants metricComponentName = metricNames.getRight();

        if ( Objects.nonNull( metricComponentName ) )
        {
            name += " " + metricComponentName;
        }

        // Are decision thresholds defined?
        Set<PoolMetadata> metadatas = parameters.metadata();
        PoolMetadata exampleMetadata = metadatas.iterator()
                                                .next();

        name += this.getEnsembleAverageTypeQualifier( parameters.ensembleAverageType(),
                                                      exampleMetadata,
                                                      metricName,
                                                      metricComponentName );

        String geoName = this.getGeoNameForTitle( exampleMetadata );

        // Qualify the geography if not all geographic features
        boolean featuresQualified = !isSummaryStatistic
                                    || !summaryStatistic.getDimensionList()
                                                        .contains( SummaryStatistic.StatisticDimension.FEATURES );
        if ( featuresQualified )
        {
            name += " at " + geoName;
        }

        String scenarioName = this.getScenarioNameForTitle( metadatas, metricName, isSummaryStatistic );
        name += " for" + scenarioName + "predictions of";

        name += " " + this.getVariableName( exampleMetadata, metricName, metricComponentName );

        if ( !this.isMultiScenarioPlot( metadatas, metricName, isSummaryStatistic ) )
        {
            name += this.getBaselineScenarioForTitle( exampleMetadata, metricName );
        }

        ChartType chartType = parameters.chartType();
        StatisticType statisticType = parameters.statisticType();
        ChronoUnit durationUnits = parameters.durationUnits();

        name += this.getTimeScaleForTitle( exampleMetadata, durationUnits );
        name += this.getTimeWindowForTitle( parameters.timeWindow(), statisticType, durationUnits );
        name += this.getThresholdForTitle( exampleMetadata, chartType, statisticType );
        name += this.getCovariateFiltersForTitle( exampleMetadata );
        name += this.getQuantilesQualifier( metricName,
                                            parameters.quantiles(),
                                            isSummaryStatistic );

        return name;
    }

    /**
     * Determines whether there is more than one scenario to plot.
     *
     * @param metadatas the metadatas to test
     * @param metric the metric
     * @param isSummaryStatistic is true if the statistic is a summary statistic
     * @return whether this is a multi-scenario plot
     */

    private boolean isMultiScenarioPlot( Set<PoolMetadata> metadatas,
                                         MetricConstants metric,
                                         boolean isSummaryStatistic )
    {
        // Must be more than one singleton pool type
        return GraphicsUtils.isStatisticForPairs( metric, isSummaryStatistic )
               && metadatas.stream()
                           .anyMatch( s -> s.getPoolDescription()
                                            .getIsBaselinePool() )
               && metadatas.stream()
                           .anyMatch( s -> !s.getPoolDescription()
                                             .getIsBaselinePool() );
    }

    /**
     * @param metric the metric
     * @param summaryStatistic the summary statistic
     * @return the name qualifier for a summary statistic
     */
    private String getSummaryStatisticQualifier( MetricConstants metric,
                                                 SummaryStatistic summaryStatistic )
    {
        String name = "";

        boolean isSummaryStatistic = Objects.nonNull( summaryStatistic )
                                     && !summaryStatistic.getDimensionList()
                                                         .contains( SummaryStatistic.StatisticDimension.RESAMPLED );

        if ( isSummaryStatistic )
        {
            String statisticName = summaryStatistic.getStatistic()
                                                   .toString()
                                                   .replace( "_", " " );

            if ( summaryStatistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
            {
                if ( metric.isInGroup( StatisticType.DURATION_SCORE ) )
                {
                    statisticName += " " + summaryStatistic.getProbability();
                }
                else
                {
                    statisticName = "QUANTILES";  // Plural
                }
            }

            // Report the dimension. If the statistic aggregates a feature group, report as features because the group
            // is qualified separately
            String dimension = this.getSummaryStatisticDimensionsQualifier( summaryStatistic );

            name = statisticName
                   + " across "
                   + dimension
                   + OF_THE;
        }

        return name;
    }

    /**
     * Returns the dimensions qualified for a summary statistic.
     * @param summaryStatistic the summary statistic
     * @return the dimensions qualifier
     */

    private String getSummaryStatisticDimensionsQualifier( SummaryStatistic summaryStatistic )
    {
        StringBuilder dimensionBuilder = new StringBuilder();
        List<SummaryStatistic.StatisticDimension> dimensionsList = summaryStatistic.getDimensionList();

        for ( int i = 0; i < dimensionsList.size(); i++ )
        {
            if ( dimensionsList.size() > 1 )
            {
                if ( i == dimensionsList.size() - 1 )
                {
                    dimensionBuilder.append( AND );
                }
                else if ( i > 0 )
                {
                    dimensionBuilder.append( ", " );
                }
            }

            String dimensionString = dimensionsList.get( i )
                                                   .toString()
                                                   .replace( "_", " " );
            dimensionBuilder.append( dimensionString );
        }

        String dimension = dimensionBuilder.toString();

        if ( dimensionsList.contains( SummaryStatistic.StatisticDimension.FEATURE_GROUP ) )
        {
            dimension = dimension.replace( SummaryStatistic.StatisticDimension.FEATURE_GROUP.toString(),
                                           SummaryStatistic.StatisticDimension.FEATURES.toString() );
        }

        return dimension;
    }

    /**
     * Returns a name qualifier for the ensemble average type.
     * @param ensembleAverageType the ensemble average type
     * @param metadata the pool metadata
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     * @return the qualifier string
     */
    private String getEnsembleAverageTypeQualifier( EnsembleAverageType ensembleAverageType,
                                                    PoolMetadata metadata,
                                                    MetricConstants metricName,
                                                    MetricConstants metricComponentName )
    {
        String name = "";

        boolean hasDecisionThresholds = metadata.hasThresholds()
                                        && metadata.getThresholds()
                                                   .hasTwo();

        // Qualify the ensemble average type if present and the metric is single-valued, unless it is a sample size or
        // a univariate metric applied to the LHS of the pairs. If the LHS eventually supports ensembles, the data
        // types will need to be qualified in the evaluation message.
        if ( Objects.nonNull( ensembleAverageType )
             && ensembleAverageType != EnsembleAverageType.NONE
             // Single-valued metrics or dichotomous metrics defined for single-valued pairs only
             && ( metricName.isInGroup( SampleDataGroup.SINGLE_VALUED )
                  || metricName.isInGroup( SampleDataGroup.SINGLE_VALUED_TIME_SERIES )
                  || ( metricName.isInGroup( SampleDataGroup.DICHOTOMOUS )
                       && !hasDecisionThresholds ) )
             && metricName != MetricConstants.SAMPLE_SIZE
             && metricComponentName != MetricConstants.OBSERVED )
        {
            name = " of the ENSEMBLE " + ensembleAverageType.name();
        }

        return name;
    }

    /**
     * Generates qualifying text about the quantile intervals for use in a chart title.
     * @param metricName the metric name
     * @param quantiles the quantiles
     * @param isSummaryStatistic true if the quantiles refer to a summary statistic, false for sampling uncertainty
     * @return the sampling uncertainty qualification
     */

    private String getQuantilesQualifier( MetricConstants metricName,
                                          SortedSet<Double> quantiles,
                                          boolean isSummaryStatistic )
    {
        String name = "";
        if ( !quantiles.isEmpty()
             && this.canShowErrorBars( metricName ) )
        {
            double minimum = quantiles.stream()
                                      .mapToDouble( Double::doubleValue )
                                      .min()
                                      .orElse( Double.NaN );

            double maximum = quantiles.stream()
                                      .mapToDouble( Double::doubleValue )
                                      .max()
                                      .orElse( Double.NaN );

            SortedSet<Double> quantileSet = new TreeSet<>();
            quantileSet.add( minimum );
            quantileSet.add( maximum );

            String quantileName = "error bars";

            if ( isSummaryStatistic )
            {
                quantileName = "distribution bars";

                // Qualify median value if available as this is also plotted
                if ( quantiles.contains( 0.5 ) )
                {
                    quantileSet.add( 0.5 );
                }
            }

            if ( metricName == MetricConstants.SAMPLE_SIZE )
            {
                return AND
                       + quantileName
                       + " for quantiles of "
                       + quantileSet
                       + ", which show the variation "
                       + "in sample size across the pool samples";
            }
            else
            {
                return AND
                       + quantileName
                       + " for quantiles of "
                       + quantileSet;
            }
        }

        return name;
    }

    /**
     * @param metricName the metric name
     * @return whether error bars can be displayed
     */

    private boolean canShowErrorBars( MetricConstants metricName )
    {
        return metricName.isSamplingUncertaintyAllowed()
               // Disallowed for graphics
               && !metricName.isInGroup( StatisticType.DURATION_SCORE )
               && !metricName.isInGroup( StatisticType.DURATION_DIAGRAM );
    }

    /**
     * Uncovers the scenario name for the plot title.
     *
     * @param metadatas the sample metadata
     * @param metric the metric name
     * @return the scenario name
     * @throws NullPointerException if the metadata or metric is null
     */

    private String getScenarioNameForTitle( Set<PoolMetadata> metadatas,
                                            MetricConstants metric,
                                            boolean isSummaryStatistic )
    {
        Objects.requireNonNull( metadatas );
        Objects.requireNonNull( metric );

        String scenarioName = " ";

        // Not univariate statistics, with exceptions
        if ( GraphicsUtils.isStatisticForPairs( metric, isSummaryStatistic ) )
        {
            String space = " ";

            PoolMetadata exampleMetadata = metadatas.iterator()
                                                    .next();
            Evaluation evaluation = exampleMetadata.getEvaluation();

            if ( this.isMultiScenarioPlot( metadatas, metric, isSummaryStatistic ) )
            {
                scenarioName = this.getMultiScenarioNameForTitle( metadatas, metric );
            }
            else if ( exampleMetadata.getPoolDescription()
                                     .getIsBaselinePool() )
            {
                if ( !evaluation.getBaselineDataName()
                                .isBlank() )
                {
                    scenarioName = space
                                   + evaluation.getBaselineDataName()
                                   + space;
                }
            }
            else if ( !evaluation.getRightDataName()
                                 .isBlank() )
            {
                scenarioName = space
                               + evaluation.getRightDataName()
                               + space;
            }
        }

        return scenarioName;
    }

    /**
     * Generates a scenario name for a multi-scenario plot.
     *
     * @param metadatas the metadatas
     * @return the scenario name
     */

    private String getMultiScenarioNameForTitle( Set<PoolMetadata> metadatas, MetricConstants metric )
    {
        String baselineScenario = BASELINE_SCENARIO_LABEL;
        String mainScenarioAppender = PREDICTED_SCENARIO_LABEL;
        if ( metric.isInGroup( StatisticType.DURATION_SCORE ) )
        {
            baselineScenario = " (lighter)";
            mainScenarioAppender = " (darker)";
        }
        else if ( metric == MetricConstants.SCATTER_PLOT )
        {
            baselineScenario = " (baseline)";
            mainScenarioAppender = " (predicted)";
        }

        String baselineScenarioFinal = baselineScenario;
        String mainScenarioAppenderFinal = mainScenarioAppender;

        Set<String> scenarioNames = metadatas.stream()
                                             .filter( s -> !s.getEvaluation()
                                                             .getRightDataName()
                                                             .isEmpty() )
                                             .map( s -> s.getEvaluation()
                                                         .getRightDataName()
                                                        + mainScenarioAppenderFinal )
                                             .collect( Collectors.toSet() );

        if ( scenarioNames.isEmpty() )
        {
            scenarioNames.add( "PREDICTED" + mainScenarioAppender );
        }

        Set<String> baselineScenarioNames = metadatas.stream()
                                                     .filter( s -> !s.getEvaluation()
                                                                     .getBaselineDataName()
                                                                     .isEmpty() )
                                                     .map( s -> s.getEvaluation()
                                                                 .getBaselineDataName()
                                                                + baselineScenarioFinal )
                                                     .collect( Collectors.toSet() );

        if ( baselineScenarioNames.isEmpty() )
        {
            scenarioNames.add( "BASELINE" + baselineScenario );
        }

        // Add the baseline scenario last
        Comparator<String> comparator = ( a, b ) ->
        {
            if ( a.endsWith( baselineScenarioFinal ) )
            {
                return 1;
            }
            else if ( b.endsWith( baselineScenarioFinal ) )
            {
                return -1;
            }

            return a.compareTo( b );
        };

        Set<String> scenarioNamesSorted = new TreeSet<>( comparator );
        scenarioNamesSorted.addAll( scenarioNames );
        scenarioNamesSorted.addAll( baselineScenarioNames );

        StringJoiner joiner = new StringJoiner( AND );
        scenarioNamesSorted.forEach( joiner::add );

        return " " + joiner + " ";
    }

    /**
     * Uncovers the variable name.
     *
     * @param metadata the sample metadata
     * @param metric the metric name
     * @param component the metric component name
     * @return the variable name
     * @throws NullPointerException if the metadata or metric is null
     */

    private String getVariableName( PoolMetadata metadata, MetricConstants metric, MetricConstants component )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( metric );

        Evaluation evaluation = metadata.getEvaluation();

        // Default to left
        String variableName = "";

        if ( Objects.nonNull( component ) )
        {
            // Get the name that corresponds to the side of the component. Again, should probably use the triple.
            switch ( component )
            {
                case PREDICTED -> variableName += evaluation.getRightVariableName();
                case BASELINE -> variableName += evaluation.getBaselineVariableName();
                default -> variableName += evaluation.getLeftVariableName();
            }
        }
        else
        {
            variableName += evaluation.getLeftVariableName();
        }

        return variableName;
    }

    /**
     * Returns the timescale of the evaluation for the plot title.
     *
     * @param metadata the statistics metadata
     * @param durationUnits the lead duration units
     * @return the timescale
     * @throws NullPointerException if either input is null
     */

    private String getTimeScaleForTitle( PoolMetadata metadata, ChronoUnit durationUnits )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( durationUnits );

        String timeScale = "";
        if ( metadata.hasTimeScale() )
        {
            timeScale = " with a time scale of ";

            // Use the default string representation of an instantaneous timescale
            // See #62867
            if ( metadata.getTimeScale().isInstantaneous() )
            {
                timeScale += metadata.getTimeScale()
                                     .toString();
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

                joiner.add( timeScaleOuter.getFunction()
                                          .toString() );

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

                timeScale += joiner.toString();
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

        if ( ( chartType == ChartType.THRESHOLD_LEAD
               && statisticType == StatisticType.DIAGRAM )
             || ( statisticType == StatisticType.BOXPLOT_PER_POOL
                  && metadata.hasThresholds()
                  && !metadata.getThresholds()
                              .first()
                              .isAllDataThreshold() ) )
        {
            threshold = " and a threshold of "
                        + metadata.getThresholds()
                                  .first()
                                  .toString();
        }

        return threshold;
    }

    /**
     * Uncovers the covariate information for the plot title.
     *
     * @param metadata the sample metadata
     * @return the threshold
     * @throws NullPointerException if any input is null
     */

    private String getCovariateFiltersForTitle( PoolMetadata metadata )
    {
        Objects.requireNonNull( metadata );

        String threshold = "";

        // Covariates with an explicit purpose of filtering
        List<Covariate> covariates = MessageUtilities.getCovariateFilters( metadata.getEvaluation()
                                                                                   .getCovariatesList() );
        if ( !covariates.isEmpty() )
        {
            threshold = " and covariate filters of: ";
            StringJoiner builder = new StringJoiner( "; " );
            String append = "";

            // Iterate the covariates and continue to add while less than the character limit
            for ( Covariate covariate : covariates )
            {
                String next = MessageUtilities.toString( covariate );
                if ( ( ( builder + "; " + next ).length() + 3 ) < COVARIATE_CHARACTER_LIMIT )
                {
                    builder.add( next );
                }
                else
                {
                    append = "...";
                    break;
                }
            }

            threshold = threshold + builder + append;
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

        if ( this.isSkillMetric( metric ) )
        {
            String baselineSuffix = metadata.getEvaluation()
                                            .getBaselineDataName();

            if ( baselineSuffix.isBlank() )
            {
                baselineSuffix = "BASELINE";
            }

            // Skill scores for baseline use a default reference, which is climatology
            // This is also potentially brittle, so consider a better way, such as adding the default baseline
            // name into the evaluation description
            if ( metadata.getPoolDescription()
                         .getIsBaselinePool() )
            {
                baselineSuffix = metadata.getEvaluation()
                                         .getDefaultBaseline()
                                         .name()
                                         .replace( "_", " " );
            }

            baselineScenario += " against predictions from " + baselineSuffix;
        }

        return baselineScenario;
    }

    /**
     * <p>Returns whether the metric is a skill metric without a default baseline.
     *
     * <p>TODO: need a less brittle way to identify skill measures that have used a default baseline vs. an explicit
     * one because a pool that includes an explicit baseline may or may not have been used for specific measures.
     *
     * @param metric the metric to test
     * @return whether the metric is a type of skill metric without a default baseline.
     */
    private boolean isSkillMetric( MetricConstants metric )
    {
        return metric.isDifferenceMetric()
               || ( metric.isSkillMetric()
                    && !metric.isInGroup( SampleDataGroup.DICHOTOMOUS )
                    && metric != MetricConstants.KLING_GUPTA_EFFICIENCY );
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

        String regionName = metadata.getPoolDescription()
                                    .getGeometryGroup()
                                    .getRegionName();

        if ( regionName.isBlank() )
        {
            regionName = "UNKNOWN";

            LOGGER.debug( "Discovered a missing region name in pool: {}.", metadata );
        }

        return regionName;
    }

    /**
     * Uncovers the time window for the plot title.
     *
     * @param timeWindow the time window
     * @param statisticType the type of statistic
     * @param durationUnits the duration units
     * @return the time window
     * @throws NullPointerException if the metadata is null
     */

    private String getTimeWindowForTitle( TimeWindowOuter timeWindow,
                                          StatisticType statisticType,
                                          ChronoUnit durationUnits )
    {
        Objects.requireNonNull( timeWindow );

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
                start = AND;
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
                start = AND;
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

        if ( !timeWindowString.isBlank() )
        {
            timeWindowString = " and a time window that has " + timeWindowString;
        }

        return timeWindowString;
    }

    /**
     * Returns a legend name from the inputs.
     * @param metricName the metric name
     * @param chartType the chart type
     * @param graphicShape the graphic shape
     * @param durationUnits the duration units
     * @param timeWindows the time windows
     * @param thresholdUnits the threshold measurement units
     * @throws NullPointerException if any input is null
     */
    private String getLegendName( MetricConstants metricName,
                                  ChartType chartType,
                                  GraphicShape graphicShape,
                                  ChronoUnit durationUnits,
                                  Set<TimeWindowOuter> timeWindows,
                                  String thresholdUnits )
    {
        Objects.requireNonNull( metricName );
        Objects.requireNonNull( thresholdUnits );
        Objects.requireNonNull( chartType );
        Objects.requireNonNull( graphicShape );
        Objects.requireNonNull( durationUnits );

        String legendTitle = "";

        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        // Special case
        // TODO: eliminate this special case and handle more generally
        if ( metricName == MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM )
        {
            legendTitle = "Name: ";
        }
        // Another special snowflake
        else if ( metricName == MetricConstants.SCATTER_PLOT )
        {
            legendTitle = "";
        }
        // Lead-threshold
        else if ( chartType == ChartType.LEAD_THRESHOLD
                  || chartType == ChartType.TIMING_ERROR_SUMMARY_STATISTICS )
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
            legendTitle = this.getPoolingWindowLegendName( metricName,
                                                           graphicShape,
                                                           durationUnits,
                                                           timeWindows,
                                                           thresholdUnits );
        }

        return legendTitle;
    }

    /**
     * Returns a legend name from the inputs for a chart that displays statistics for each of several pools.
     * @param metricName the metric name
     * @param graphicShape the graphic shape
     * @param durationUnits the duration units
     * @param timeWindows the time windows
     * @param thresholdUnits the threshold measurement units
     * @throws NullPointerException if any input is null
     */
    private String getPoolingWindowLegendName( MetricConstants metricName,
                                               GraphicShape graphicShape,
                                               ChronoUnit durationUnits,
                                               Set<TimeWindowOuter> timeWindows,
                                               String thresholdUnits )
    {
        Objects.requireNonNull( timeWindows );
        Objects.requireNonNull( graphicShape );
        Objects.requireNonNull( durationUnits );

        String legendTitle = "";

        String leadUnits = durationUnits.toString()
                                        .toUpperCase();

        // Plots for scores contain all pools, so qualify the time components if needed
        if ( metricName.isInGroup( StatisticType.DOUBLE_SCORE )
             || metricName.isInGroup( StatisticType.DURATION_SCORE ) )
        {
            // Only qualify time window dimensions in the legend when the dimension has more than one bookend that differs
            // across pools, i.e., no singletons because these are qualified in the chart title
            // Lead durations where required
            boolean qualifyLead = timeWindows.stream()
                                             .map( TimeWindowOuter::getEarliestLeadDuration )
                                             .collect( Collectors.toSet() )
                                             .size() > 1
                                  || timeWindows.stream()
                                                .map( TimeWindowOuter::getLatestLeadDuration )
                                                .collect( Collectors.toSet() )
                                                .size() > 1;
            if ( qualifyLead )
            {
                legendTitle = legendTitle + "Lead time window [" + leadUnits + "], ";
            }

            // Valid times when required
            boolean qualifyValid = graphicShape != GraphicShape.VALID_DATE_POOLS
                                   && ( timeWindows.stream()
                                                   .map( TimeWindowOuter::getEarliestValidTime )
                                                   .collect( Collectors.toSet() )
                                                   .size() > 1
                                        || timeWindows.stream()
                                                      .map( TimeWindowOuter::getLatestValidTime )
                                                      .collect( Collectors.toSet() )
                                                      .size() > 1 );
            if ( qualifyValid )
            {
                legendTitle = legendTitle + "Valid time window [UTC], ";
            }

            // Reference times when required
            boolean qualifyReference = graphicShape != GraphicShape.ISSUED_DATE_POOLS
                                       && ( timeWindows.stream()
                                                       .map( TimeWindowOuter::getEarliestReferenceTime )
                                                       .collect( Collectors.toSet() )
                                                       .size() > 1
                                            || timeWindows.stream()
                                                          .map( TimeWindowOuter::getLatestReferenceTime )
                                                          .collect( Collectors.toSet() )
                                                          .size() > 1 );
            if ( qualifyReference )
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
        if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS
             || graphicShape == GraphicShape.VALID_DATE_POOLS )
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
            case BOXPLOT_PER_PAIR, BOXPLOT_PER_POOL, DURATION_DIAGRAM, PAIRS -> ChartType.UNIQUE;
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
     * Generates a name qualifier for a diagram summary statistic. The qualifier contains the name of the metric being
     * summarized.
     * @param statistic the statistic
     * @return the name qualifier
     */

    private String getDiagramStatisticNameQualifier( DiagramStatisticOuter statistic )
    {
        String qualifier = "";
        // For a diagram summary statistic, the underlying metric being summarized is recorded separately
        DiagramMetric metric = statistic.getStatistic()
                                        .getMetric();
        if ( statistic.isSummaryStatistic()
             && metric.getStatisticName() != MetricName.UNDEFINED )
        {
            qualifier = statistic.getStatistic()
                                 .getMetric()
                                 .getStatisticName()
                                 .toString()
                                 .replace( "_", " " );

            if ( GraphicsUtils.isNotDefaultMetricComponentName( metric.getStatisticComponentName() ) )
            {
                qualifier = metric.getStatisticComponentName()
                                  .name()
                                  .replace( "_", " " )
                            + OF_THE
                            + qualifier;
            }
        }

        return qualifier;
    }

    /**
     * @param example an example dataset
     * @return the duration diagram domain axis title
     */

    private String getDurationDiagramDomainTitle( DurationDiagramStatisticOuter example )
    {
        ReferenceTime.ReferenceTimeType type = example.getStatistic()
                                                      .getReferenceTimeType();

        if ( type == ReferenceTime.ReferenceTimeType.UNKNOWN )
        {
            return "REFERENCE TIME OF UNKNOWN TYPE [UTC]";
        }

        return type.toString()
                   .replace( "_", " " )
               + " [UTC]";
    }

    /**
     * Returns the common time window boundaries, which is different from the {@link TimeWindowSlicer#union(Set)}.
     * @param timeWindows the time windows
     * @return the common boundaries
     */
    private TimeWindowOuter getAggregateTimeWindow( Set<TimeWindowOuter> timeWindows )
    {
        // Start with the unbounded window
        TimeWindow.Builder builder = MessageUtilities.getTimeWindow()
                                                     .toBuilder();

        SortedSet<Instant> earliestReference =
                timeWindows.stream()
                           .map( s -> MessageUtilities.getInstant( s.getTimeWindow()
                                                                    .getEarliestReferenceTime() ) )
                           .collect( Collectors.toCollection( TreeSet::new ) );
        SortedSet<Instant> latestReference =
                timeWindows.stream()
                           .map( s -> MessageUtilities.getInstant( s.getTimeWindow()
                                                                    .getLatestReferenceTime() ) )
                           .collect( Collectors.toCollection( TreeSet::new ) );
        SortedSet<Instant> earliestValid =
                timeWindows.stream()
                           .map( s -> MessageUtilities.getInstant( s.getTimeWindow()
                                                                    .getEarliestValidTime() ) )
                           .collect( Collectors.toCollection( TreeSet::new ) );
        SortedSet<Instant> latestValid =
                timeWindows.stream()
                           .map( s -> MessageUtilities.getInstant( s.getTimeWindow()
                                                                    .getLatestValidTime() ) )
                           .collect( Collectors.toCollection( TreeSet::new ) );
        SortedSet<Duration> earliestLead =
                timeWindows.stream()
                           .map( s -> MessageUtilities.getDuration( s.getTimeWindow()
                                                                     .getEarliestLeadDuration() ) )
                           .collect( Collectors.toCollection( TreeSet::new ) );
        SortedSet<Duration> latestLead =
                timeWindows.stream()
                           .map( s -> MessageUtilities.getDuration( s.getTimeWindow()
                                                                     .getLatestLeadDuration() ) )
                           .collect( Collectors.toCollection( TreeSet::new ) );

        if ( earliestReference.size() == 1 )
        {
            builder.setEarliestReferenceTime( MessageUtilities.getTimestamp( earliestReference.first() ) );
        }
        if ( latestReference.size() == 1 )
        {
            builder.setLatestReferenceTime( MessageUtilities.getTimestamp( latestReference.first() ) );
        }
        if ( earliestValid.size() == 1 )
        {
            builder.setEarliestValidTime( MessageUtilities.getTimestamp( earliestValid.first() ) );
        }
        if ( latestValid.size() == 1 )
        {
            builder.setLatestValidTime( MessageUtilities.getTimestamp( latestValid.first() ) );
        }
        if ( earliestLead.size() == 1 )
        {
            builder.setEarliestLeadDuration( MessageUtilities.getDuration( earliestLead.first() ) );
        }
        if ( latestLead.size() == 1 )
        {
            builder.setLatestLeadDuration( MessageUtilities.getDuration( latestLead.first() ) );
        }

        return TimeWindowOuter.of( builder.build() );
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

    /**
     * Small value class for chart title creation.
     * @param metadata the metadatas
     * @param timeWindow the time window to use for the chart title, possibly an aggregate
     * @param metricNames the metric names
     * @param durationUnits the duration units
     * @param chartType the chart type
     * @param statisticType the statistic type
     * @param ensembleAverageType the ensemble average type
     * @param quantiles the quantiles
     * @param summaryStatistic the summary statistic
     * @param summaryStatisticNameQualifier the name qualifier for a summary statistic
     */
    private record ChartTitleParameters( Set<PoolMetadata> metadata,
                                         TimeWindowOuter timeWindow,
                                         Pair<MetricConstants, MetricConstants> metricNames,
                                         ChronoUnit durationUnits,
                                         ChartType chartType,
                                         StatisticType statisticType,
                                         EnsembleAverageType ensembleAverageType,
                                         SortedSet<Double> quantiles,
                                         SummaryStatistic summaryStatistic,
                                         String summaryStatisticNameQualifier )
    {
        /**
         * Validate inputs.
         * @param metadata the metadata
         * @param metricNames the metric names
         * @param durationUnits the duration units
         * @param chartType the chart type
         * @param statisticType the statistic type
         * @param ensembleAverageType the ensemble average type
         * @param quantiles the quantiles
         * @param summaryStatistic the summary statistic
         * @param summaryStatisticNameQualifier the name qualifier for a diagram summary statistic
         */
        private ChartTitleParameters
        {
            Objects.requireNonNull( metadata );
            Objects.requireNonNull( metricNames );
            Objects.requireNonNull( durationUnits );
            Objects.requireNonNull( chartType );
            Objects.requireNonNull( statisticType );
        }
    }
}
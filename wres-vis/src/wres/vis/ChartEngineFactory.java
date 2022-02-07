package wres.vis;

import java.awt.Font;
import java.awt.FontFormatException;
import java.awt.GraphicsEnvironment;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.DefaultXYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.datasource.instances.CategoricalXYChartDataSource;
import ohd.hseb.charter.datasource.instances.DataSetXYChartDataSource;
import ohd.hseb.charter.datasource.instances.NumericalXYChartDataSource;
import ohd.hseb.charter.parameters.ChartDrawingParameters;
import ohd.hseb.charter.parameters.SubPlotParameters;
import ohd.hseb.charter.parameters.SubtitleParameters;
import ohd.hseb.charter.parameters.ThresholdParameters;
import ohd.hseb.hefs.utils.arguments.ArgumentsProcessor;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import ohd.hseb.hefs.utils.xml.XMLTools;
import wres.config.generated.OutputTypeSelection;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricDimension;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;


/**
 * Factory to use in order to construct a wres-vis chart.
 * 
 * @author Hank.Herr
 */
public abstract class ChartEngineFactory
{
    private static final String BOX_PLOT_OF_ERRORS_TEMPLATE_XML = "boxPlotOfErrorsTemplate.xml";
    private static final Logger LOGGER = LoggerFactory.getLogger( ChartEngineFactory.class );

    /**
     * Corresponds to {@link OutputTypeSelection} but includes others to make it more useful within the maps inside this factory
     * and in identifying special cases, such as pooling windows.  Note that every {@link OutputTypeSelection} that can be specified
     * (other than DEFAULT) must have a corresponding value in this enum, but not the other way.
     * @author Hank.Herr
     *
     */
    public enum ChartType
    {
        UNIQUE( null ),
        LEAD_THRESHOLD( OutputTypeSelection.LEAD_THRESHOLD ),
        THRESHOLD_LEAD( OutputTypeSelection.THRESHOLD_LEAD ),
        POOLING_WINDOW( null ), // Internal type only, not configured
        SINGLE_VALUED_PAIRS( OutputTypeSelection.SINGLE_VALUED_PAIRS );

        private final OutputTypeSelection basis;

        ChartType( OutputTypeSelection v )
        {
            basis = v;
        }

        /**
         * @return The {@link OutputTypeSelection} corresponding to this.  If null, then there is no specific matching {@link OutputTypeSelection}.
         */
        public OutputTypeSelection getBasis()
        {
            return basis;
        }

        public boolean isFor( OutputTypeSelection outputType )
        {
            return outputType == basis;
        }
    }

    /**
     * Provides the default {@link ChartType} for a given {@link StatisticType}.
     * That chart type can then be used in the other maps to determine the default template file name.
     * Thus, the values from this map must be kept consistent with the template maps.
     * Any chart type selection of {@link ChartType#UNIQUE} indicates that the chart type doesn't matter for that metric
     * group, likely because the chart type is fixed for all metrics in that metric group.
     */
    private static EnumMap<StatisticType, ChartType> metricOutputGroupToDefaultChartTypeMap =
            new EnumMap<>( StatisticType.class );
    static
    {
        metricOutputGroupToDefaultChartTypeMap.put( StatisticType.BOXPLOT_PER_PAIR, ChartType.UNIQUE );
        metricOutputGroupToDefaultChartTypeMap.put( StatisticType.BOXPLOT_PER_POOL, ChartType.UNIQUE );
        metricOutputGroupToDefaultChartTypeMap.put( StatisticType.DOUBLE_SCORE, ChartType.LEAD_THRESHOLD );
        metricOutputGroupToDefaultChartTypeMap.put( StatisticType.DURATION_SCORE, ChartType.UNIQUE );
        metricOutputGroupToDefaultChartTypeMap.put( StatisticType.DIAGRAM, ChartType.LEAD_THRESHOLD );
        metricOutputGroupToDefaultChartTypeMap.put( StatisticType.DURATION_DIAGRAM, ChartType.UNIQUE );
    }

    /**
     * Maps metrics to their templates.  If a metric does not have an entry in this map, then the template
     * depends on the chart type; use the {@link #chartTypeSpecificTemplateMap}.
     */
    private static EnumMap<MetricConstants, String> metricSpecificTemplateMap =
            new EnumMap<>( MetricConstants.class );
    static
    {
        metricSpecificTemplateMap.put( MetricConstants.RELIABILITY_DIAGRAM, "reliabilityDiagramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                       "rocDiagramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.QUANTILE_QUANTILE_DIAGRAM, "qqDiagramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM, "qqDiagramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.RANK_HISTOGRAM, "rankHistogramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                       BOX_PLOT_OF_ERRORS_TEMPLATE_XML );
        metricSpecificTemplateMap.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                       BOX_PLOT_OF_ERRORS_TEMPLATE_XML );
        metricSpecificTemplateMap.put( MetricConstants.BOX_PLOT_OF_ERRORS, BOX_PLOT_OF_ERRORS_TEMPLATE_XML );
        metricSpecificTemplateMap.put( MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS, BOX_PLOT_OF_ERRORS_TEMPLATE_XML );
        metricSpecificTemplateMap.put( MetricConstants.TIME_TO_PEAK_ERROR, "timeToPeakErrorTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR, "timeToPeakErrorTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                       "timeToPeakSummaryStatsTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC,
                                       "timeToPeakSummaryStatsTemplate.xml" );
    }

    /**
     * Maps chart types to templates.   
     */
    private static EnumMap<ChartType, String> chartTypeSpecificTemplateMap =
            new EnumMap<>( ChartType.class );
    static
    {
        chartTypeSpecificTemplateMap.put( ChartType.LEAD_THRESHOLD, "scalarOutputLeadThresholdTemplate.xml" );
        chartTypeSpecificTemplateMap.put( ChartType.THRESHOLD_LEAD, "scalarOutputThresholdLeadTemplate.xml" );
        chartTypeSpecificTemplateMap.put( ChartType.POOLING_WINDOW, "scalarOutputPoolingWindowTemplate.xml" );
    }

    /**
     * Per SonarLint, hiding the public constructor.
     */
    private ChartEngineFactory()
    {
    }

    /**
     * @param metricName the metric name.
     * @param graphicShape The graphic shape.
     * @return The {@link OutputTypeSelection} specifying the output type for the plot.  
     * @throws NullPointerException if any input is null
     */
    private static ChartType determineChartType( MetricConstants metricName,
                                                 GraphicShape graphicShape )
    {
        Objects.requireNonNull( graphicShape );
        Objects.requireNonNull( metricName );

        //Pooling window case.
        if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS || graphicShape == GraphicShape.VALID_DATE_POOLS )
        {
            return ChartType.POOLING_WINDOW;
        }

        //All others.  If user specified nothing, pull from the map.  Otherwise base it on the user
        //specified type.
        if ( graphicShape == GraphicShape.DEFAULT )
        {
            return ChartEngineFactory.metricOutputGroupToDefaultChartTypeMap.get( metricName.getMetricOutputGroup() );
        }

        return ChartType.valueOf( graphicShape.name() );
    }

    /**
     * @param metricId Metric
     * @param outputType Chart type.
     * @return The template file name corresponding to the metric and chart type.  It uses {@link #metricOutputGroupToDefaultChartTypeMap} first and,
     * if nothing is found, then uses {@link #chartTypeSpecificTemplateMap}.  If nothing is found, still, it throws an {@link IllegalArgumentException}:
     * the code is passing in an unsupported combination of metric and chart type.
     */
    private static String determineTemplate( final MetricConstants metricId,
                                             final ChartType outputType )
    {
        String results = metricSpecificTemplateMap.get( metricId );
        if ( results == null )
        {
            results = chartTypeSpecificTemplateMap.get( outputType );
            if ( results == null )
            {
                throw new IllegalArgumentException( "Plot template for metric " + metricId
                                                    + " with output type "
                                                    + outputType
                                                    + " is not supported." );
            }
        }
        return results;
    }

    /**
     * For diagrams only, which use {@link DiagramStatisticOuter}, slice by key type and decompose by component name.
     * @param inputKeyInstance The key-instance corresponding to the slice to create.
     * @param input The input from which to draw the data.
     * @param usedPlotType The plot type.
     * @return A single input slice for use in drawing the diagram.
     */
    private static List<DiagramStatisticOuter> sliceInputForDiagram( Object inputKeyInstance,
                                                                     final List<DiagramStatisticOuter> input,
                                                                     OutputTypeSelection usedPlotType )
    {
        List<DiagramStatisticOuter> inputSlice;
        if ( usedPlotType == OutputTypeSelection.LEAD_THRESHOLD )
        {
            inputSlice =
                    Slicer.filter( input, next -> next.getMetadata().getTimeWindow().equals( inputKeyInstance ) );
        }
        else if ( usedPlotType == OutputTypeSelection.THRESHOLD_LEAD )
        {
            inputSlice =
                    Slicer.filter( input, next -> next.getMetadata().getThresholds().equals( inputKeyInstance ) );
        }
        else
        {
            throw new IllegalArgumentException( "Plot type " + usedPlotType
                                                + " is invalid for this diagram." );
        }

        // One diagram for each qualifier name in the diagram statistic. This accounts for diagrams that contain the
        // same metric dimensions multiple times, such as ensemble QQ diagrams
        return ChartEngineFactory.getDecomposedDiagrams( inputSlice );
    }

    /**
     * @param diagrams the diagrams to decompose
     * @return one diagram for each qualifier name
     */

    private static List<DiagramStatisticOuter> getDecomposedDiagrams( List<DiagramStatisticOuter> diagrams )
    {
        List<DiagramStatisticOuter> returnMe = new ArrayList<>();

        for ( DiagramStatisticOuter diagram : diagrams )
        {
            SortedSet<MetricDimension> names = diagram.getComponentNames();
            SortedSet<String> qualifiers = diagram.getComponentNameQualifiers();

            DiagramStatistic.Builder builder = diagram.getData().toBuilder();
            for ( String qualifier : qualifiers )
            {
                List<DiagramStatisticComponent> components = new ArrayList<>();

                for ( MetricDimension name : names )
                {
                    DiagramStatisticComponent next = diagram.getComponent( name, qualifier );
                    components.add( next );
                }

                // Add the new diagram for this qualifier
                builder = builder.clearStatistics();
                builder.addAllStatistics( components );

                DiagramStatistic newDiagram = builder.build();
                DiagramStatisticOuter newWrappedDiagram = DiagramStatisticOuter.of( newDiagram, diagram.getMetadata() );
                returnMe.add( newWrappedDiagram );
            }
        }

        LOGGER.debug( "Decomposed {} diagrams into {} diagrams for plotting.", diagrams.size(), returnMe.size() );

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * For diagrams only, which use {@link DiagramStatisticOuter}.
     * @param inputKeyInstance The key-instance corresponding to the slice to create.
     * @param inputSlice The input slice from which to draw the data.
     * @param usedPlotType The plot type.
     * @param ChronoUnit durationUnits the time units
     * @param hasRepeatedComponents is true for diagrams that have repeated components, false otherwise
     * @return the argument processor
     */
    private static ArgumentProcessor constructDiagramArguments( Object inputKeyInstance,
                                                                List<DiagramStatisticOuter> inputSlice,
                                                                ChartType usedPlotType,
                                                                ChronoUnit durationUnits,
                                                                boolean hasRepeatedComponents )
    {
        ArgumentProcessor args = new ArgumentProcessor( inputSlice.get( 0 ).getMetricName(),
                                                        null,
                                                        null,
                                                        inputSlice,
                                                        usedPlotType,
                                                        durationUnits );
        if ( usedPlotType.equals( ChartType.LEAD_THRESHOLD ) )
        {
            args.addLeadThresholdArguments( inputSlice, (TimeWindowOuter) inputKeyInstance, hasRepeatedComponents );
        }
        else if ( usedPlotType == ChartType.THRESHOLD_LEAD )
        {
            args.addThresholdLeadArguments( inputSlice, (OneOrTwoThresholds) inputKeyInstance, hasRepeatedComponents );
        }
        else
        {
            throw new IllegalArgumentException( "Plot type " + usedPlotType
                                                + " is invalid for this diagram." );
        }
        return args;
    }


    /**
     * Constructs a reliability diagram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @return A {@link JFreeChart} instance.
     * @param durationUnits the duration units
     * @throws ChartBuildingException If the chart fails to construct.
     */
    private static JFreeChart processReliabilityDiagram( Object inputKeyInstance,
                                                         List<DiagramStatisticOuter> input,
                                                         ChartType usedPlotType,
                                                         String templateName,
                                                         ChronoUnit durationUnits )
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final List<DiagramStatisticOuter> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        ArgumentProcessor arguments =
                constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType, durationUnits, false );

        dataSources.add( XYChartDataSourceFactory.ofVerificationDiagram( 0,
                                                                         inputSlice,
                                                                         MetricDimension.FORECAST_PROBABILITY,
                                                                         MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                         MetricDimension.FORECAST_PROBABILITY.toString(),
                                                                         MetricDimension.OBSERVED_RELATIVE_FREQUENCY.toString(),
                                                                         0,
                                                                         null,
                                                                         durationUnits ) );
        dataSources.add( XYChartDataSourceFactory.ofVerificationDiagram( 1,
                                                                         inputSlice,
                                                                         MetricDimension.FORECAST_PROBABILITY,
                                                                         MetricDimension.SAMPLE_SIZE,
                                                                         MetricDimension.FORECAST_PROBABILITY.toString(),
                                                                         MetricDimension.SAMPLE_SIZE.toString(),
                                                                         1,
                                                                         null,
                                                                         durationUnits ) );
        //Diagonal data source added so that it shows up in the legend.
        dataSources.add( constructConnectedPointsDataSource( 2,
                                                             0,
                                                             new Point2D.Double( 0.0, 0.0 ),
                                                             new Point2D.Double( 1.0, 1.0 ) ) );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName, inputSlice.size() );

        //Build the ChartEngine instance.
        return generateJFreeChart( dataSources,
                                   arguments,
                                   parameters,
                                   diagonalDataSourceIndices,
                                   axisToSquareAgainstDomain );
    }


    /**
     * Constructs a ROC diagram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param durationUnits the duration units
     * @return A {@link JFreeChart} instance.
     * @throws ChartBuildingException If the chart fails to construct.
     */
    private static JFreeChart processROCDiagram( Object inputKeyInstance,
                                                 List<DiagramStatisticOuter> input,
                                                 ChartType usedPlotType,
                                                 String templateName,
                                                 ChronoUnit durationUnits )
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final List<DiagramStatisticOuter> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        ArgumentProcessor arguments =
                constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType, durationUnits, false );

        dataSources.add( XYChartDataSourceFactory.ofVerificationDiagram( 0,
                                                                         inputSlice,
                                                                         MetricDimension.PROBABILITY_OF_FALSE_DETECTION,
                                                                         MetricDimension.PROBABILITY_OF_DETECTION,
                                                                         MetricDimension.PROBABILITY_OF_FALSE_DETECTION.toString(),
                                                                         MetricDimension.PROBABILITY_OF_DETECTION.toString(),
                                                                         0,
                                                                         null,
                                                                         durationUnits ) );
        dataSources.add( constructConnectedPointsDataSource( 1,
                                                             0,
                                                             new Point2D.Double( 0.0, 0.0 ),
                                                             new Point2D.Double( 1.0, 1.0 ) ) );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName, inputSlice.size() );

        //Build the ChartEngine instance.
        return generateJFreeChart( dataSources,
                                   arguments,
                                   parameters,
                                   diagonalDataSourceIndices,
                                   axisToSquareAgainstDomain );
    }


    /**
     * Constructs a QQ diagram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param durationUnits the duration units
     * @return A {@link JFreeChart} instance.
     * @throws ChartBuildingException If the chart fails to construct.
     */
    private static JFreeChart processQQDiagram( Object inputKeyInstance,
                                                List<DiagramStatisticOuter> input,
                                                ChartType usedPlotType,
                                                String templateName,
                                                ChronoUnit durationUnits )
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final List<DiagramStatisticOuter> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        ArgumentProcessor arguments =
                constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType, durationUnits, false );

        DefaultXYChartDataSource dataSource = XYChartDataSourceFactory.ofVerificationDiagram( 0,
                                                                                              inputSlice,
                                                                                              MetricDimension.OBSERVED_QUANTILES,
                                                                                              MetricDimension.PREDICTED_QUANTILES,
                                                                                              MetricConstants.MetricDimension.OBSERVED_QUANTILES.toString()
                                                                                                                                   + " @variableName@@inputUnitsLabelSuffix@",
                                                                                              MetricConstants.MetricDimension.PREDICTED_QUANTILES.toString() + " @variableName@@inputUnitsLabelSuffix@",
                                                                                              0,
                                                                                              null,
                                                                                              durationUnits );

        //Diagonal data source added, but it won't show up in the legend since it uses features of WRESChartEngine.
        //Also squaring the axes.
        diagonalDataSourceIndices = new int[] { 1 };
        axisToSquareAgainstDomain = ChartConstants.YAXIS_XML_STRINGS[ChartConstants.LEFT_YAXIS_INDEX];
        dataSources.add( dataSource );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName, inputSlice.size() );

        //Build the ChartEngine instance.
        return generateJFreeChart( dataSources,
                                   arguments,
                                   parameters,
                                   diagonalDataSourceIndices,
                                   axisToSquareAgainstDomain );
    }

    /**
     * Constructs an ensemble QQ diagram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param durationUnits the duration units
     * @return A {@link JFreeChart} instance.
     * @throws ChartBuildingException If the chart fails to construct.
     */
    private static JFreeChart processEnsembleQQDiagram( Object inputKeyInstance,
                                                        List<DiagramStatisticOuter> input,
                                                        ChartType usedPlotType,
                                                        String templateName,
                                                        ChronoUnit durationUnits )
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final List<DiagramStatisticOuter> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        ArgumentProcessor arguments =
                constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType, durationUnits, true );

        XYChartDataSource dataSource = XYChartDataSourceFactory.ofVerificationDiagram( 0,
                                                                                       inputSlice,
                                                                                       MetricDimension.OBSERVED_QUANTILES,
                                                                                       MetricDimension.PREDICTED_QUANTILES,
                                                                                       MetricConstants.MetricDimension.OBSERVED_QUANTILES.toString()
                                                                                                                            + " @variableName@@inputUnitsLabelSuffix@",
                                                                                       MetricConstants.MetricDimension.PREDICTED_QUANTILES.toString() + " @variableName@@inputUnitsLabelSuffix@",
                                                                                       0,
                                                                                       null,
                                                                                       durationUnits );

        //Diagonal data source added, but it won't show up in the legend since it uses features of WRESChartEngine.
        //Also squaring the axes.
        diagonalDataSourceIndices = new int[] { 1 };
        axisToSquareAgainstDomain = ChartConstants.YAXIS_XML_STRINGS[ChartConstants.LEFT_YAXIS_INDEX];
        dataSources.add( dataSource );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName, inputSlice.size() );

        //Build the ChartEngine instance.
        return generateJFreeChart( dataSources,
                                   arguments,
                                   parameters,
                                   diagonalDataSourceIndices,
                                   axisToSquareAgainstDomain );
    }

    /**
     * Constructs a rank histogram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param durationUnits the duration units
     * @return A {@link JFreeChart} instance.
     * @throws ChartBuildingException If the chart fails to construct.
     */
    private static JFreeChart processRankHistogram( Object inputKeyInstance,
                                                    List<DiagramStatisticOuter> input,
                                                    ChartType usedPlotType,
                                                    String templateName,
                                                    ChronoUnit durationUnits )
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final List<DiagramStatisticOuter> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        ArgumentProcessor arguments =
                constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType, durationUnits, false );

        Supplier<XYDataset> dataSupplier = () -> new RankHistogramXYDataset( inputSlice,
                                                                             MetricDimension.RANK_ORDER,
                                                                             MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                             durationUnits );

        dataSources.add( XYChartDataSourceFactory.ofVerificationDiagram( 0,
                                                                         inputSlice,
                                                                         MetricDimension.RANK_ORDER,
                                                                         MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                         "Bin Separating Ranked Ensemble Members",
                                                                         MetricDimension.OBSERVED_RELATIVE_FREQUENCY.toString(),
                                                                         0,
                                                                         dataSupplier,
                                                                         durationUnits ) );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName, inputSlice.size() );

        //Build the ChartEngine instance.
        return generateJFreeChart( dataSources,
                                   arguments,
                                   parameters,
                                   diagonalDataSourceIndices,
                                   axisToSquareAgainstDomain );
    }

    /**Calls the process methods as appropriate for the given plot type.
     * @param input The metric output to plot.
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return A map of charts.
     * @throws ChartBuildingException If the chart fails to construct.
     */
    public static ConcurrentMap<Object, JFreeChart> buildDiagramChartEngine( List<DiagramStatisticOuter> input,
                                                                             GraphicShape graphicShape,
                                                                             ChronoUnit durationUnits )
    {
        final ConcurrentMap<Object, JFreeChart> results = new ConcurrentSkipListMap<>();

        // Find the metadata for the first element, which is sufficient here
        MetricConstants metricName = input.get( 0 ).getMetricName();

        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = ChartEngineFactory.determineChartType( metricName, graphicShape );

        String templateName = ChartEngineFactory.determineTemplate( metricName,
                                                                    usedPlotType );

        //Determine the key set for the loop below based on if this is a lead time first and threshold first plot type.
        Set<Object> keySetValues =
                Slicer.discover( input, next -> next.getMetadata().getTimeWindow() );
        if ( usedPlotType.isFor( OutputTypeSelection.THRESHOLD_LEAD ) )
        {
            keySetValues = Slicer.discover( input, next -> next.getMetadata().getThresholds() );
        }

        //For each key instance, do the following....
        for ( final Object keyInstance : keySetValues )
        {
            JFreeChart engine;
            switch ( metricName )
            {
                case RELIABILITY_DIAGRAM:
                    engine =
                            ChartEngineFactory.processReliabilityDiagram( keyInstance,
                                                                          input,
                                                                          usedPlotType,
                                                                          templateName,
                                                                          durationUnits );
                    break;
                case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                    engine =
                            ChartEngineFactory.processROCDiagram( keyInstance,
                                                                  input,
                                                                  usedPlotType,
                                                                  templateName,
                                                                  durationUnits );
                    break;
                case QUANTILE_QUANTILE_DIAGRAM:
                    engine =
                            ChartEngineFactory.processQQDiagram( keyInstance,
                                                                 input,
                                                                 usedPlotType,
                                                                 templateName,
                                                                 durationUnits );
                    break;
                case ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM:
                    engine =
                            ChartEngineFactory.processEnsembleQQDiagram( keyInstance,
                                                                         input,
                                                                         usedPlotType,
                                                                         templateName,
                                                                         durationUnits );
                    break;
                case RANK_HISTOGRAM:
                    engine =
                            ChartEngineFactory.processRankHistogram( keyInstance,
                                                                     input,
                                                                     usedPlotType,
                                                                     templateName,
                                                                     durationUnits );
                    break;
                default:
                    throw new IllegalArgumentException( "Unrecognized plot type of " + metricName
                                                        + " specified in the metric information." );
            }
            results.put( keyInstance, engine );
        }
        return results;
    }


    /**
     * 
     * @param input Input providing the box plot data.
     * @param templateName The name of the template to use, if not the standard template.
     * @param durationUnits the duration units
     * @return A single instance of {@link JFreeChart}.
     * @throws ChartBuildingException If the chart could not build for whatever reason.
     */
    private static JFreeChart processBoxPlotErrorsDiagram( List<BoxplotStatisticOuter> input,
                                                           String templateName,
                                                           ChronoUnit durationUnits )
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        ArgumentProcessor arguments = null;
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;


        MetricConstants metricName = input.get( 0 ).getMetricName();

        if ( !metricName.isInGroup( StatisticType.BOXPLOT_PER_PAIR )
             && !metricName.isInGroup( StatisticType.BOXPLOT_PER_POOL ) )
        {
            throw new IllegalArgumentException( "Unrecognized data type for metric " + metricName
                                                + ". Expected one of "
                                                + StatisticType.BOXPLOT_PER_PAIR
                                                + " or "
                                                + StatisticType.BOXPLOT_PER_POOL
                                                + ", but got "
                                                + metricName.getMetricOutputGroup()
                                                + "." );
        }

        arguments = new ArgumentProcessor( input.get( 0 ), durationUnits );

        //Add the data source
        dataSources.add( XYChartDataSourceFactory.ofBoxPlotOutput( 0, input, null, durationUnits ) );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName, 1 );

        //Build the ChartEngine instance.
        return ChartEngineFactory.generateJFreeChart( dataSources,
                                                      arguments,
                                                      parameters,
                                                      diagonalDataSourceIndices,
                                                      axisToSquareAgainstDomain );
    }

    /**
     * Builds a box plot for each {@link BoxplotStatisticOuter} in the input.
     * @param input The metric output to plot.
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return a map of charts.
     * @throws ChartBuildingException If the chart fails to construct.
     */
    public static Map<Pair<TimeWindowOuter, OneOrTwoThresholds>, JFreeChart>
            buildBoxPlotChartEnginePerPool( List<BoxplotStatisticOuter> input,
                                            GraphicShape graphicShape,
                                            ChronoUnit durationUnits )
    {
        final Map<Pair<TimeWindowOuter, OneOrTwoThresholds>, JFreeChart> results = new ConcurrentSkipListMap<>();

        // Find the metadata for the first element, which is sufficient here
        MetricConstants metricName = input.get( 0 ).getMetricName();

        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = ChartEngineFactory.determineChartType( metricName, graphicShape );

        String templateName = ChartEngineFactory.determineTemplate( metricName,
                                                                    usedPlotType );

        //For each input in the list, create a chart
        for ( BoxplotStatisticOuter next : input )
        {
            // Skip empty outputs: #65503
            if ( next.getData().getStatisticsCount() != 0 )
            {
                JFreeChart engine = ChartEngineFactory.processBoxPlotErrorsDiagram( List.of( next ),
                                                                                    templateName,
                                                                                    durationUnits );
                results.put( Pair.of( next.getMetadata().getTimeWindow(),
                                      next.getMetadata().getThresholds() ),
                             engine );
            }
            else
            {
                LOGGER.debug( "Skipped the box plot outputs for {} because there were no box plot statistics to draw.",
                              next.getMetadata() );
            }
        }

        return Collections.unmodifiableMap( results );
    }

    /**
     * Builds a box plot for the input.
     * 
     * @param input The metric output to plot.
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return A {@link JFreeChart} instance.
     * @throws ChartBuildingException If the chart fails to construct.
     * @throws NullPointerException if the config, input or durationUnits is null
     */
    public static JFreeChart buildBoxPlotChartEngine( List<BoxplotStatisticOuter> input,
                                                      GraphicShape graphicShape,
                                                      ChronoUnit durationUnits )
    {
        Objects.requireNonNull( input );

        Objects.requireNonNull( durationUnits );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot generate box plot graphics because no box plots statistics "
                                                + "were available." );
        }

        //Determine the output type
        // Find the metadata for the first element, which is sufficient here
        MetricConstants metricName = input.get( 0 ).getMetricName();
        ChartType usedPlotType = ChartEngineFactory.determineChartType( metricName, graphicShape );


        String templateName = ChartEngineFactory.determineTemplate( metricName, usedPlotType );

        return ChartEngineFactory.processBoxPlotErrorsDiagram( input,
                                                               templateName,
                                                               durationUnits );
    }


    /**
     * Builds a {@link ChartEngine} for each component of a score.
     * 
     * @param input The metric output to plot.
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return a map of {@link JFreeChart} containing the plots
     * @throws ChartBuildingException if the chart fails to construct
     */
    public static ConcurrentMap<MetricConstants, JFreeChart>
            buildScoreOutputChartEngine( List<DoubleScoreStatisticOuter> input,
                                         GraphicShape graphicShape,
                                         ChronoUnit durationUnits )
    {
        final ConcurrentMap<MetricConstants, JFreeChart> results = new ConcurrentSkipListMap<>();

        final Map<MetricConstants, List<DoubleScoreComponentOuter>> slicedInput =
                Slicer.filterByMetricComponent( input );
        for ( final Map.Entry<MetricConstants, List<DoubleScoreComponentOuter>> entry : slicedInput.entrySet() )
        {
            JFreeChart engine = buildScoreOutputChartEngineForOneComponent( input.get( 0 ).getMetricName(),
                                                                            entry.getValue(),
                                                                            graphicShape,
                                                                            durationUnits );
            results.put( entry.getKey(), engine );
        }
        return results;
    }

    /**
     * Internal helper that builds a {@link ChartEngine} for one score component.
     * 
     * @param metricName the metric name.
     * @param input The metric output to plot.   
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return A {@link JFreeChart} instance.
     * @throws ChartBuildingException If the chart fails to construct.
     */
    private static JFreeChart buildScoreOutputChartEngineForOneComponent( MetricConstants metricName,
                                                                          List<DoubleScoreComponentOuter> input,
                                                                          GraphicShape graphicShape,
                                                                          ChronoUnit durationUnits )
    {

        // Find the metadata for the first element, which is sufficient here
        PoolMetadata metadata = input.get( 0 ).getMetadata();
        String metricUnits = input.get( 0 ).getData().getMetric().getUnits();

        // Component name
        MetricConstants metricComponentName = input.get( 0 ).getMetricName();

        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = ChartEngineFactory.determineChartType( metricName, graphicShape );

        String templateName = ChartEngineFactory.determineTemplate( metricName,
                                                                    usedPlotType );

        //Setup the default arguments.
        final ArgumentProcessor arguments = new ArgumentProcessor( metricName,
                                                                   metricComponentName,
                                                                   metricUnits,
                                                                   input,
                                                                   usedPlotType,
                                                                   durationUnits );

        // Setup plot specific arguments for skill metrics that use an explicit baseline (dichotomous scores do not).
        arguments.addBaselineArguments( metadata, metricName );

        //Build the source.
        XYChartDataSource source = null;

        //Lead-threshold is the default.
        if ( usedPlotType == ChartType.LEAD_THRESHOLD )
        {
            source = XYChartDataSourceFactory.ofDoubleScoreOutputByLeadAndThreshold( 0, input, durationUnits );
            arguments.addLeadThresholdArguments( input, null, false );
        }
        //This is for plots with the threshold on the domain axis and lead time in the legend.
        else if ( usedPlotType == ChartType.THRESHOLD_LEAD )
        {
            source = XYChartDataSourceFactory.ofDoubleScoreOutputByThresholdAndLead( 0, input, durationUnits );
            arguments.addThresholdLeadArguments( input, null, false );
        }
        //This is for plots that operate with sequences of time windows (e.g. rolling windows)
        else if ( usedPlotType == ChartType.POOLING_WINDOW )
        {
            source = XYChartDataSourceFactory.ofDoubleScoreOutputByPoolingWindow( 0,
                                                                                  input,
                                                                                  durationUnits,
                                                                                  graphicShape );

            arguments.addPoolingWindowArguments( input, graphicShape );
        }
        else
        {
            throw new IllegalArgumentException( "Chart type of " + usedPlotType
                                                + " is not valid for a generic scalar output plot; must be one of "
                                                + ChartType.LEAD_THRESHOLD
                                                + ", "
                                                + ChartType.THRESHOLD_LEAD
                                                + ", "
                                                + ChartType.POOLING_WINDOW
                                                + "." );
        }

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName,
                                                          source.getNumberOfSeries() );

        //Build the ChartEngine instance.
        return generateJFreeChart( List.of( source ),
                                   arguments,
                                   parameters,
                                   null,
                                   null );
    }


    /**
     * Only usable with {@link DurationDiagramStatisticOuter} in which the left is {@link Instant} and the right is {@link Duration}.
     * 
     * @param input the statistics to plot.
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return {@link JFreeChart} ready for plot production.
     * @throws ChartBuildingException If the chart fails to build for any reason.
     */
    public static JFreeChart buildDurationDiagramChartEngine( List<DurationDiagramStatisticOuter> input,
                                                              GraphicShape graphicShape,
                                                              ChronoUnit durationUnits )
    {
        // Find the metadata for the first element, which is sufficient here
        MetricConstants metricName = input.get( 0 ).getMetricName();

        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = ChartEngineFactory.determineChartType( metricName, graphicShape );

        String templateName = ChartEngineFactory.determineTemplate( metricName,
                                                                    usedPlotType );

        //Setup the default arguments.
        final ArgumentProcessor arguments = new ArgumentProcessor( metricName,
                                                                   null,
                                                                   durationUnits.toString(),
                                                                   input,
                                                                   null,
                                                                   durationUnits );

        PoolMetadata metadata = input.get( 0 ).getMetadata();

        //Setup plot specific arguments.
        arguments.addDurationMetricArguments();
        arguments.addTimeToPeakArguments( input );
        arguments.addBaselineArguments( metadata, metricName );

        //Build the source.
        XYChartDataSource source = XYChartDataSourceFactory.ofPairedOutputInstantDuration( 0, input );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName,
                                                          source.getNumberOfSeries() );

        //Build the ChartEngine instance.
        return generateJFreeChart( List.of( source ),
                                   arguments,
                                   parameters,
                                   null,
                                   null );
    }


    /**
     * 
     * @param input The input for which to build the categorical plot.
     * @param graphicShape The shape of the graphic to plot.
     * @param durationUnits the duration units
     * @return A {@link JFreeChart} ready to be used to build a chart.
     * @throws ChartBuildingException A problem was encountered building the chart.
     */
    public static JFreeChart buildCategoricalDurationScoreChartEngine( List<DurationScoreStatisticOuter> input,
                                                                       GraphicShape graphicShape,
                                                                       ChronoUnit durationUnits )
    {
        // Find the metadata for the first element, which is sufficient here
        MetricConstants metricName = input.get( 0 ).getMetricName();

        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = ChartEngineFactory.determineChartType( metricName, graphicShape );

        String templateName = ChartEngineFactory.determineTemplate( metricName,
                                                                    usedPlotType );

        //Setup the default arguments.
        final ArgumentProcessor arguments = new ArgumentProcessor( metricName,
                                                                   null,
                                                                   durationUnits.toString(),
                                                                   input,
                                                                   null,
                                                                   durationUnits );

        PoolMetadata metadata = input.get( 0 ).getMetadata();

        //Setup plot specific arguments.
        arguments.addDurationMetricArguments();
        arguments.addTimeToPeakArguments( input );
        arguments.addBaselineArguments( metadata, metricName );

        //Setup the assumed source and arguments.
        CategoricalXYChartDataSource source =
                XYChartDataSourceFactory.ofDurationScoreCategoricalOutput( 0, input );

        // Create the chart parameters
        ChartParameters parameters = new ChartParameters( templateName,
                                                          source.getNumberOfSeries() );

        //Build the ChartEngine instance.
        return generateJFreeChart( List.of( source ),
                                   arguments,
                                   parameters,
                                   null,
                                   null );
    }

    /**
     * @param input The pairs to plot.
     * @param durationUnits the duration units
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    public static ChartEngine buildSingleValuedPairsChartEngine( final Pool<Pair<Double, Double>> input,
                                                                 final ChronoUnit durationUnits )
            throws ChartEngineException
    {

        String templateName = "singleValuedPairsTemplate.xml";

        //Build the source.
        final DefaultXYChartDataSource source = XYChartDataSourceFactory.ofSingleValuedPairs( 0, input );

        //Setup the arguments.
        final ArgumentProcessor arguments =
                new ArgumentProcessor( input.getMetadata(),
                                       input.getMetadata().getMeasurementUnit().toString(),
                                       durationUnits );

        //Build the ChartEngine instance.
        return ChartTools.buildChartEngine( List.of( source ),
                                            arguments,
                                            templateName,
                                            null );
    }

    /**
     * Adds a new data source, which can then be modified (in appearance) by a user. This should only be used for very
     * small datasets, as the class it makes use of, {@link DataSetXYChartDataSource} makes a duplicate of the data in
     * order to create an {@link XYDataset}.
     * 
     * @param sourceIndex The index of the new data source.
     * @param points The points to connect via lines, probably, but its up to the template/user to determine.
     * @return An instance of DataSetXYChartDataSource initialized accordingly.
     * @throws XYChartDataSourceException Currently, this is a place holder from the stuff called in case a subclass is
     *             used that needs to throw an exception. However, this will not get thrown as of now.
     */
    private static XYChartDataSource constructConnectedPointsDataSource( final int sourceIndex,
                                                                         final int subPlotIndex,
                                                                         final Point2D... points )
    {
        final double[] xValues = new double[points.length];
        final double[] yValues = new double[points.length];
        for ( int i = 0; i < points.length; i++ )
        {
            xValues[i] = points[i].getX();
            yValues[i] = points[i].getY();
        }
        try
        {
            final NumericalXYChartDataSource source = new NumericalXYChartDataSource( null,
                                                                                      sourceIndex,
                                                                                      List.of( xValues ),
                                                                                      List.of( yValues ) );
            source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "" );
            source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( "" );
            source.getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex( subPlotIndex );
            return source;
        }
        catch ( final XYChartDataSourceException e )
        {
            throw new IllegalStateException( "The DataSetXYChartDataSource does not throw exceptions, so how did I get here?" );
        }
    }


    /**
     * Method to call in order to generate an instance of {@link WRESChartEngine}.
     * 
     * @param dataSources The data sources to include.
     * @param arguments The arguments to use.
     * @param chartParameters the chart parameters
     * @param diagonalDataSourceIndices The indices of the data sources used to define the appearance of the diagonals.
     *            ONLY supply these indices if you want the chart engine to add the diagonals through JFreeChart tools;
     *            such diagonals will never appear in the legend, only on the plot. If you need the diagonal to appear
     *            on the legend and on the plot, then you must define it through a regular data source, likely making
     *            use of the constructConnectedPointsDataSource method.
     * @param axisToSquareAgainstDomain A string indicating the axes to square against the domain; either "left" or
     *            "right".
     * @return A {@link JFreeChart} instance ready to use.
     * @throws ChartBuildingException If the chart could not be created.
     */

    public static JFreeChart generateJFreeChart( final List<XYChartDataSource> dataSources,
                                                 final ArgumentsProcessor arguments,
                                                 final ChartParameters chartParameters,
                                                 final int[] diagonalDataSourceIndices,
                                                 final String axisToSquareAgainstDomain )
    {
        try
        {
            ChartEngine engine = new WRESChartEngine( dataSources,
                                                      arguments,
                                                      chartParameters.getParameters(),
                                                      diagonalDataSourceIndices,
                                                      axisToSquareAgainstDomain );

            ChartEngineFactory.prepareChartEngineForWriting( engine );

            return engine.buildChart();
        }
        catch ( ChartEngineException | FontFormatException | XYChartDataSourceException | IOException e )
        {
            throw new ChartBuildingException( "Encountered an error while building a chart.", e );
        }
    }

    /**
     * Overrides the appearance of the chart to support comparisons between SVG and image outputs across platforms.  
     * 
     * @param engine The charting engine to modify.  This is modified in place.
     * @throws FontLoadingException If the liberation font, captured in LiberationSans-Regular.ttf, 
     *            which should be on the classpath when this is executed, cannot be found.
     * @throws IOException The font file cannot be opened and used to create a font for whatever reason.
     * @throws FontFormatException The format of the font information cannot be understood.
     * @throws FontLoadingException If the fon could not be loaded.
     */
    private static void prepareChartEngineForWriting( ChartEngine engine )
            throws IOException, FontFormatException
    {
        // #81628 change.  Create the chart and force it to use a Liberation Sans font.  Below is a test run.
        // It will need to be enhanced later to modify *all* fonts in the JFreeChart.
        // Create the chart
        String fontResource = "LiberationSans-Regular.ttf";
        URL fontUrl = ChartEngineFactory.class.getClassLoader().getResource( fontResource );

        // Load the font and force it into the chart.
        if ( Objects.isNull( fontUrl ) )
        {
            throw new FontLoadingException( "Could not find the " + fontResource
                                            + " file on the class path." );
        }

        try
        {
            // Create from file, not stream
            // https://stackoverflow.com/questions/38783010/huge-amount-of-jf-tmp-files-in-var-cache-tomcat7-temp
            File fontFile = new File( fontUrl.toURI() );
            Font font = Font.createFont( Font.TRUETYPE_FONT, fontFile ).deriveFont( 10.0f );

            // Register font with graphics env
            GraphicsEnvironment graphics = GraphicsEnvironment.getLocalGraphicsEnvironment();
            graphics.registerFont( font );

            //Set all ChartEngine fonts to be the liberation font with size 10.
            engine.getChartParameters().getPlotTitle().setFont( font );
            engine.getChartParameters().getLegend().getLegendTitle().setFont( font );
            engine.getChartParameters().getLegend().getLegendEntryFont().setFont( font );
            engine.getChartParameters().getDomainAxis().getLabel().setFont( font ); //One shared domain axis.
            for ( SubPlotParameters subPlot : engine.getChartParameters().getSubPlotParameters() ) //Range axes by subplot.
            {
                subPlot.getLeftAxis().getLabel().setFont( font ); //Font is used for axis label and tick marks.
                subPlot.getRightAxis().getLabel().setFont( font ); //Font is used for axis label and ticks marks.
            }
            for ( SubtitleParameters parms : engine.getChartParameters().getSubtitleList().getSubtitleList() )
            {
                parms.setFont( font );
            }
            for ( ThresholdParameters parms : engine.getChartParameters()
                                                    .getThresholdList()
                                                    .getThresholdParametersList() )
            {
                parms.getLabel().setFont( font );
            }
        }
        catch ( URISyntaxException e )
        {
            throw new IOException( "While attempting to read a font.", e );
        }
    }


    /**
     * Creates a small bag of chart parameters from input strings in XML format.
     * 
     * @author Hank.Herr
     * @author James Brown
     */
    private static class ChartParameters
    {
        /** The main parameters. */
        private final ChartDrawingParameters mainParameters;

        /**
         * @return the main parameters.
         */

        private ChartDrawingParameters getParameters()
        {
            return this.mainParameters;
        }

        /**
         * Create an instance.
         * @param templateName The name of the template system resource or file. It will be loaded here.
         * @param seriesCount The number of series. If greater than 100, no legend is visible.
         * @throws ChartBuildingException if the parameters could not be created from the strings
         */

        private ChartParameters( String templateName, int seriesCount )
        {
            // Load the template parameters.  This will first attempt to load them as a system resource on the class 
            // path and then as a file from the file system.  If neither works, it throws an exception.
            final ChartDrawingParameters parameters = new ChartDrawingParameters();

            InputStream templateStream =
                    ChartEngineFactory.class.getClassLoader()
                                            .getResourceAsStream( templateName );

            try
            {
                if ( templateStream != null )
                {
                    XMLTools.readXMLFromStream( templateStream, false, parameters );
                }
                else
                {
                    //XMLTools.readXMLFromFile( new File( templateName ),
                    XMLTools.readXMLFromFile( new File( templateName ).getAbsoluteFile(),
                                              parameters );
                }
            }
            catch ( GenericXMLReadingHandlerException e )
            {
                throw new ChartBuildingException( "Unable to load default chart drawing parameters from resource "
                                                  + "or file with name '"
                                                  + templateName
                                                  + "': ",
                                                  e );
            }
            finally
            {
                if ( templateStream != null )
                {
                    try
                    {
                        templateStream.close();
                    }
                    catch ( IOException ioe )
                    {
                        // Failure to close should not affect primary outputs.
                        LOGGER.warn( "Failed to close template stream {}",
                                     templateStream,
                                     ioe );
                    }
                }
            }

            // Turn off the legend?
            if ( seriesCount > 100 )
            {
                LOGGER.debug( "While creating a chart, discovered a dataset with more than 100 series ({}). "
                              + "Overriding the chart drawing parameters to turn off the legend.",
                              seriesCount );

                parameters.getLegend().setVisible( false );
            }

            this.mainParameters = parameters;
        }
    }


}

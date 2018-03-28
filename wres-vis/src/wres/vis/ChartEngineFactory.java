package wres.vis;

import java.awt.geom.Point2D;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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
import ohd.hseb.hefs.utils.arguments.ArgumentsProcessor;
import ohd.hseb.hefs.utils.xml.XMLTools;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.OneOrTwoThresholds;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;

/**
 * Factory to use in order to construct a wres-vis chart.
 * 
 * @author Hank.Herr
 */
public abstract class ChartEngineFactory
{
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
        POOLING_WINDOW( null ), //OutputTypeSelection.POOLING_WINDOW will go away.
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

        public static ChartType fromOutputTypeSelection( OutputTypeSelection v )
        {
            if ( v != null )
            {
                for ( ChartType c : ChartType.values() )
                {
                    if ( v.equals( c.getBasis() ) )
                    {
                        return c;
                    }
                }
            }
            throw new IllegalArgumentException( "OutputTypeSelection " + v.toString()
                                                + " cannot be translated to a ChartEngineFactory ChartType." );
        }
    }

    /**
     * Provides the default {@link ChartType} for a given {@link MetricOutputGroup}.
     * That chart type can then be used in the other maps to determine the default template file name.
     * Thus, the values from this map must be kept consistent with the template maps.
     * Any chart type selection of {@link ChartType#UNIQUE} indicates that the chart type doesn't matter for that metric
     * group, likely because the chart type is fixed for all metrics in that metric group.
     */
    private static EnumMap<MetricOutputGroup, ChartType> metricOutputGroupToDefaultChartTypeMap =
            new EnumMap<>( MetricOutputGroup.class );
    static
    {
        metricOutputGroupToDefaultChartTypeMap.put( MetricOutputGroup.BOXPLOT, ChartType.UNIQUE );
        metricOutputGroupToDefaultChartTypeMap.put( MetricOutputGroup.DOUBLE_SCORE, ChartType.LEAD_THRESHOLD );
        metricOutputGroupToDefaultChartTypeMap.put( MetricOutputGroup.DURATION_SCORE, ChartType.UNIQUE );
        metricOutputGroupToDefaultChartTypeMap.put( MetricOutputGroup.MATRIX, ChartType.UNIQUE );
        metricOutputGroupToDefaultChartTypeMap.put( MetricOutputGroup.MULTIVECTOR, ChartType.LEAD_THRESHOLD );
        metricOutputGroupToDefaultChartTypeMap.put( MetricOutputGroup.PAIRED, ChartType.UNIQUE );
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
        metricSpecificTemplateMap.put( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM, "rocDiagramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.QUANTILE_QUANTILE_DIAGRAM, "qqDiagramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.RANK_HISTOGRAM, "rankHistogramTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE, "boxPlotOfErrorsTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE, "boxPlotOfErrorsTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.TIME_TO_PEAK_ERROR, "timeToPeakErrorTemplate.xml" );
        metricSpecificTemplateMap.put( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC, "timeToPeakSummaryStatsTemplate.xml" );
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
     * @param config The project configuration.
     * @param input The input provided for charting, which is the metric output.
     * @param userSpecifiedOutputType A user specified plot type; null means the user did not provide one.
     * @return The {@link OutputTypeSelection} specifying the output type for the plot.  
     */
    private static ChartType determineChartType(ProjectConfig config,
                                                MetricOutputMapByTimeAndThreshold<?> input,
                                                OutputTypeSelection userSpecifiedOutputType )
    {
        //Pooling window case.
        if ( Objects.nonNull( config ) && Objects.nonNull( config.getPair() ) && Objects.nonNull( config.getPair().getIssuedDatesPoolingWindow()))
        {
            return ChartType.POOLING_WINDOW;
        }
        
        //All others.  If user specified nothing, pull from the map.  Otherwise base it on the user
        //specified type.
        if ( ( userSpecifiedOutputType == null ) || (userSpecifiedOutputType == OutputTypeSelection.DEFAULT) )
        {
            return metricOutputGroupToDefaultChartTypeMap.get( input.getMetadata()
                                                                    .getMetricID()
                                                                    .getMetricOutputGroup() );
        }
        return ChartType.fromOutputTypeSelection( userSpecifiedOutputType );
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
     * For diagrams only, which use {@link MultiVectorOutput}.
     * @param inputKeyInstance The key-instance corresponding to the slice to create.
     * @param input The input from which to draw the data.
     * @param usedPlotType The plot type.
     * @return A single input slice for use in drawing the diagram.
     */
    private static MetricOutputMapByTimeAndThreshold<MultiVectorOutput> sliceInputForDiagram( Object inputKeyInstance,
                                                                                              final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                                                                                              OutputTypeSelection usedPlotType )
    {
        MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice;
        if ( usedPlotType == OutputTypeSelection.LEAD_THRESHOLD )
        {

            inputSlice = input.filterByTime( (TimeWindow) inputKeyInstance );
        }
        else if ( usedPlotType == OutputTypeSelection.THRESHOLD_LEAD )
        {
            inputSlice =
                    input.filterByThreshold( (OneOrTwoThresholds) inputKeyInstance );
        }
        else
        {
            throw new IllegalArgumentException( "Plot type " + usedPlotType
                                                + " is invalid for this diagram." );
        }
        return inputSlice;
    }

    /**
     * For diagrams only, which use {@link MultiVectorOutput}.
     * @param inputKeyInstance The key-instance corresponding to the slice to create.
     * @param inputSlice The input slice from which to draw the data.
     * @param usedPlotType The plot type.
     * @return
     */
    private static WRESArgumentProcessor constructDiagramArguments( Object inputKeyInstance,
                                                                    MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice,
                                                                    ChartType usedPlotType )
    {
        WRESArgumentProcessor args = new WRESArgumentProcessor( inputSlice, usedPlotType );
        if ( usedPlotType.equals( ChartType.LEAD_THRESHOLD ) )
        {
            args.addLeadThresholdArguments( inputSlice, (TimeWindow) inputKeyInstance );
        }
        else if ( usedPlotType == ChartType.THRESHOLD_LEAD )
        {
            args.addThresholdLeadArguments( inputSlice, (OneOrTwoThresholds) inputKeyInstance );
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
     * @param factory The data factory from which arguments will be identified.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} to be stored with the inputKeyInstance in a results map.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    private static WRESChartEngine
            processReliabilityDiagram(
                                       Object inputKeyInstance,
                                       final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                                       final DataFactory factory,
                                       ChartType usedPlotType,
                                       String templateName,
                                       String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        WRESArgumentProcessor arguments = constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType );

        dataSources.add( XYChartDataSourceFactory.ofMultiVectorOutputDiagram( 0,
                                                                              inputSlice,
                                                                              MetricDimension.FORECAST_PROBABILITY,
                                                                              MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                              MetricDimension.FORECAST_PROBABILITY.toString(),
                                                                              MetricDimension.OBSERVED_RELATIVE_FREQUENCY.toString(),
                                                                              0,
                                                                              null ) );
        dataSources.add( XYChartDataSourceFactory.ofMultiVectorOutputDiagram( 1,
                                                                              inputSlice,
                                                                              MetricDimension.FORECAST_PROBABILITY,
                                                                              MetricDimension.SAMPLE_SIZE,
                                                                              MetricDimension.FORECAST_PROBABILITY.toString(),
                                                                              MetricDimension.SAMPLE_SIZE.toString(),
                                                                              1,
                                                                              null ) );
        //Diagonal data source added so that it shows up in the legend.
        dataSources.add( constructConnectedPointsDataSource( 2,
                                                             0,
                                                             new Point2D.Double( 0.0, 0.0 ),
                                                             new Point2D.Double( 1.0, 1.0 ) ) );


        //Build the ChartEngine instance.
        return generateChartEngine( dataSources,
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    diagonalDataSourceIndices,
                                    axisToSquareAgainstDomain );
    }


    /**
     * Constructs a ROC diagram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} to be stored with the inputKeyInstance in a results map.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    private static WRESChartEngine
            processROCDiagram(
                               Object inputKeyInstance,
                               final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                               final DataFactory factory,
                               ChartType usedPlotType,
                               String templateName,
                               String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        WRESArgumentProcessor arguments = constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType );

        dataSources.add( XYChartDataSourceFactory.ofMultiVectorOutputDiagram( 0,
                                                                              inputSlice,
                                                                              MetricDimension.PROBABILITY_OF_FALSE_DETECTION,
                                                                              MetricDimension.PROBABILITY_OF_DETECTION,
                                                                              MetricDimension.PROBABILITY_OF_FALSE_DETECTION.toString(),
                                                                              MetricDimension.PROBABILITY_OF_DETECTION.toString(),
                                                                              0,
                                                                              null ) );
        dataSources.add( constructConnectedPointsDataSource( 1,
                                                             0,
                                                             new Point2D.Double( 0.0, 0.0 ),
                                                             new Point2D.Double( 1.0, 1.0 ) ) );


        //Build the ChartEngine instance.
        return generateChartEngine( dataSources,
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    diagonalDataSourceIndices,
                                    axisToSquareAgainstDomain );
    }


    /**
     * Constructs a QQ diagram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} to be stored with the inputKeyInstance in a results map.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    private static WRESChartEngine
            processQQDiagram(
                              Object inputKeyInstance,
                              final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                              final DataFactory factory,
                              ChartType usedPlotType,
                              String templateName,
                              String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        WRESArgumentProcessor arguments = constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType );

        DefaultXYChartDataSource dataSource = XYChartDataSourceFactory.ofMultiVectorOutputDiagram( 0,
                                                                                                   inputSlice,
                                                                                                   MetricDimension.OBSERVED_QUANTILES,
                                                                                                   MetricDimension.PREDICTED_QUANTILES,
                                                                                                   MetricConstants.MetricDimension.OBSERVED_QUANTILES.toString()
                                                                                                                                        + " @variableName@@inputUnitsLabelSuffix@",
                                                                                                   MetricConstants.MetricDimension.PREDICTED_QUANTILES.toString() + " @variableName@@inputUnitsLabelSuffix@",
                                                                                                   0,
                                                                                                   null );

        //Diagonal data source added, but it won't show up in the legend since it uses features of WRESChartEngine.
        //Also squaring the axes.
        diagonalDataSourceIndices = new int[] { 1 };
        axisToSquareAgainstDomain = ChartConstants.YAXIS_XML_STRINGS[ChartConstants.LEFT_YAXIS_INDEX];
        dataSources.add( dataSource );

        //Build the ChartEngine instance.
        return generateChartEngine( dataSources,
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    diagonalDataSourceIndices,
                                    axisToSquareAgainstDomain );
    }


    /**
     * Constructs a rank histogram chart.
     * @param inputKeyInstance The key-instance for which to build the plot.  The key is one of potentially multiple keys within the input provided next.
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param usedPlotType Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param templateName The name of the template to use based on the plot type.  
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} to be stored with the inputKeyInstance in a results map.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    private static WRESChartEngine
            processRankHistogram(
                                  Object inputKeyInstance,
                                  final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                                  final DataFactory factory,
                                  ChartType usedPlotType,
                                  String templateName,
                                  String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice =
                sliceInputForDiagram( inputKeyInstance, input, usedPlotType.getBasis() );
        WRESArgumentProcessor arguments = constructDiagramArguments( inputKeyInstance, inputSlice, usedPlotType );

        dataSources.add( XYChartDataSourceFactory.ofMultiVectorOutputDiagram( 0,
                                                                              inputSlice,
                                                                              MetricDimension.RANK_ORDER,
                                                                              MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                              "Bin Separating Ranked Ensemble Members",
                                                                              MetricDimension.OBSERVED_RELATIVE_FREQUENCY.toString(),
                                                                              0,
                                                                              new Supplier<XYDataset>()
                                                                              {
                                                                                  @Override
                                                                                  public XYDataset get()
                                                                                  {
                                                                                      return new RankHistogramXYDataset( inputSlice,
                                                                                                                         MetricDimension.RANK_ORDER,
                                                                                                                         MetricDimension.OBSERVED_RELATIVE_FREQUENCY );
                                                                                  }
                                                                              } ) );

        //Build the ChartEngine instance.
        return generateChartEngine( dataSources,
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    diagonalDataSourceIndices,
                                    axisToSquareAgainstDomain );
    }

    /**Calls the process methods as appropriate for the given plot type.
     * @param config The project configuration.
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param userSpecifiedPlotType An optional plot type to generate, where multiple plot types are supported for the
     *            input. May be null.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A map of keys to ChartEngine instances.  The instances can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.  The keys depend on the provided plot type selection.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    public static ConcurrentMap<Object, ChartEngine>
            buildMultiVectorOutputChartEngine( final ProjectConfig config, 
                                               final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                                               final DataFactory factory,
                                               final OutputTypeSelection userSpecifiedPlotType,
                                               final String userSpecifiedTemplateResourceName,
                                               final String overrideParametersStr )
                    throws ChartEngineException
    {
        final ConcurrentMap<Object, ChartEngine> results = new ConcurrentSkipListMap<>();

        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = determineChartType( config, input, userSpecifiedPlotType );
        String templateName = determineTemplate( input.getMetadata().getMetricID(),
                                                 usedPlotType );
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Determine the key set for the loop below based on if this is a lead time first and threshold first plot type.
        Set<?> keySetValues = input.setOfTimeWindowKey();
        if ( usedPlotType.isFor( OutputTypeSelection.THRESHOLD_LEAD ) )
        {
            keySetValues = input.setOfThresholdKey();
        }

        //For each key instance, do the following....
        for ( final Object keyInstance : keySetValues )
        {
            ChartEngine engine;
            switch ( input.getMetadata().getMetricID() )
            {
                case RELIABILITY_DIAGRAM:
                    engine =
                            processReliabilityDiagram( keyInstance,
                                                       input,
                                                       factory,
                                                       usedPlotType,
                                                       templateName,
                                                       overrideParametersStr );
                    break;
                case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                    engine =
                            processROCDiagram( keyInstance,
                                               input,
                                               factory,
                                               usedPlotType,
                                               templateName,
                                               overrideParametersStr );
                    break;
                case QUANTILE_QUANTILE_DIAGRAM:
                    engine =
                            processQQDiagram( keyInstance,
                                              input,
                                              factory,
                                              usedPlotType,
                                              templateName,
                                              overrideParametersStr );
                    break;
                case RANK_HISTOGRAM:
                    engine =
                            processRankHistogram( keyInstance,
                                                  input,
                                                  factory,
                                                  usedPlotType,
                                                  templateName,
                                                  overrideParametersStr );
                    break;
                default:
                    throw new IllegalArgumentException( "Unrecognized plot type of " + input.getMetadata().getMetricID()
                                                        + " specified in the metric information." );
            }
            results.put( keyInstance, engine );
        }
        return results;
    }


    /**
     * 
     * @param inputKeyInstance The key that will be used to find the box plot data in the provided input.
     * @param input Input providing the box plot data.
     * @param templateName The name of the template to use, if not the standard template.
     * @param overrideParametersStr An XML string providing override parameters if they were given in the project configuration.
     * @return A single instance of {@link WRESChartEngine}.
     * @throws ChartEngineException If the chart could not build for whatever reason.
     */
    private static WRESChartEngine
            processBoxPlotErrorsDiagram(
                                         Pair<TimeWindow, OneOrTwoThresholds> inputKeyInstance,
                                         final MetricOutputMapByTimeAndThreshold<BoxPlotOutput> input,
                                         String templateName,
                                         String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        WRESArgumentProcessor arguments = null;
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        if ( input.getMetadata().getMetricID() != MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE
             && input.getMetadata().getMetricID() != MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
        {
            throw new IllegalArgumentException( "Unrecognized plot type of " + input.getMetadata().getMetricID()
                                                + " specified in the metric information." );
        }

        BoxPlotOutput boxPlotData = input.get( inputKeyInstance );
        arguments = new WRESArgumentProcessor( inputKeyInstance, boxPlotData );

        //Add the data source
        dataSources.add( XYChartDataSourceFactory.ofBoxPlotOutput( 0, boxPlotData, null ) );

        //Build the ChartEngine instance.
        return generateChartEngine( dataSources,
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    diagonalDataSourceIndices,
                                    axisToSquareAgainstDomain );
    }

    /**
     * At this time, there is only one plot type available for box plots, so the user specified plot type is not included as an argument.
     * @param config The project configuration.
     * @param input The metric output to plot.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return Map where the keys are instances of {@link Pair} with the two keys being an integer and a threshold.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    public static ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, ChartEngine>
            buildBoxPlotChartEngine( final ProjectConfig config, 
                                     final MetricOutputMapByTimeAndThreshold<BoxPlotOutput> input,
                                     final String userSpecifiedTemplateResourceName,
                                     final String overrideParametersStr )
                    throws ChartEngineException
    {
        final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, ChartEngine> results = new ConcurrentSkipListMap<>();

        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = determineChartType( config, input, null );
        String templateName = determineTemplate( input.getMetadata().getMetricID(),
                                                 usedPlotType );
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Determine the key set for the loop below based on if this is a lead time first and threshold first plot type.
        Set<Pair<TimeWindow, OneOrTwoThresholds>> keySetValues = input.keySet();

        //For each lead time, do the following....
        for ( final Pair<TimeWindow, OneOrTwoThresholds> keyInstance : keySetValues )
        {
            if ( input.getMetadata().getMetricID() == MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE
                 || input.getMetadata().getMetricID() == MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
            {

                final ChartEngine engine = processBoxPlotErrorsDiagram( keyInstance,
                                                                        input,
                                                                        templateName,
                                                                        overrideParametersStr );
                results.put( keyInstance, engine );
            }

            //===================================================
            //Unrecognized metric.
            //===================================================
            else
            {
                throw new IllegalArgumentException( "Unrecognized metric of " + input.getMetadata().getMetricID()
                                                    + " specified in the metric information for the box plot chart." );
            }
        }
        return results;
    }


    /**
     * Builds a {@link ChartEngine} for each component of a score.
     * 
     * @param config The project configuration.
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param userSpecifiedPlotType An optional plot type to generate, where multiple plot types are supported for the
     *            input. May be null.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static maps.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return a map of {@link ChartEngine} containing the plots
     * @throws ChartEngineException if the ChartEngine fails to construct
     */
    public static ConcurrentMap<MetricConstants, ChartEngine>
            buildScoreOutputChartEngine( final ProjectConfig config, 
                                         final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input,
                                         final DataFactory factory,
                                         final OutputTypeSelection userSpecifiedPlotType,
                                         final String userSpecifiedTemplateResourceName,
                                         final String overrideParametersStr )
                    throws ChartEngineException
    {
        final ConcurrentMap<MetricConstants, ChartEngine> results = new ConcurrentSkipListMap<>();

        final Map<MetricConstants, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> slicedInput =
                factory.getSlicer()
                       .filterByMetricComponent( input );
        for ( final Map.Entry<MetricConstants, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> entry : slicedInput.entrySet() )
        {
            final ChartEngine engine = buildScoreOutputChartEngine( config, 
                                                                    entry.getValue(),
                                                                    userSpecifiedPlotType,
                                                                    userSpecifiedTemplateResourceName,
                                                                    overrideParametersStr );
            results.put( entry.getKey(), engine );
        }
        return results;
    }

    /**
     * Internal helper that builds a {@link ChartEngine} for one score component.
     * 
     * @param config The project configuration.
     * @param input The metric output to plot.
     * @param userSpecifiedPlotType The plot type to generate.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static map/table.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    private static ChartEngine
            buildScoreOutputChartEngine( final ProjectConfig config,
                                         final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input,
                                         final OutputTypeSelection userSpecifiedPlotType,
                                         final String userSpecifiedTemplateResourceName,
                                         final String overrideParametersStr )
                    throws ChartEngineException
    {
        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = determineChartType( config, input, userSpecifiedPlotType );
        String templateName = determineTemplate( input.getMetadata().getMetricID(),
                                                 usedPlotType );
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Setup the default arguments.
        final MetricOutputMetadata meta = input.getMetadata();
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor( input, usedPlotType );

        //Setup plot specific arguments.
        arguments.addBaselineArguments( meta );

        //Build the source.
        XYChartDataSource source = null;

        //Lead-threshold is the default.
        if ( usedPlotType == ChartType.LEAD_THRESHOLD )
        {
            source = XYChartDataSourceFactory.ofDoubleScoreOutputByLeadAndThreshold( 0, input );
            arguments.addLeadThresholdArguments( input, null );
        }
        //This is for plots with the threshold on the domain axis and lead time in the legend.
        else if ( usedPlotType == ChartType.THRESHOLD_LEAD )
        {
            source = XYChartDataSourceFactory.ofDoubleScoreOutputByThresholdAndLead( 0, input );
            arguments.addThresholdLeadArguments( input, null );
        }
        //This is for plots that operate with sequences of time windows (e.g. rolling windows)
        else if ( usedPlotType == ChartType.POOLING_WINDOW )
        {
            source = XYChartDataSourceFactory.ofDoubleScoreOutputByPoolingWindow( 0, input );
            arguments.addPoolingWindowArguments( input );
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

        //Build the ChartEngine instance.
        return generateChartEngine( Lists.newArrayList( source ),
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    null,
                                    null );
    }


    /**
     * Only usable with {@link PairedOutput} in which the left is {@link Instant} and the right is {@link Duration}.
     * @param config The project configuration.
     * @param input The input from which to build the plot.
     * @param userSpecifiedTemplateResourceName Template resource name, or null to use default.
     * @param overrideParametersStr Override template XML string, or null to not use.
     * @return {@link ChartEngine} ready for plot production.
     * @throws ChartEngineException If the {@link ChartEngine} fails to build for any reason.
     */
    public static ChartEngine
            buildPairedInstantDurationChartEngine( final ProjectConfig config,
                                                   MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> input,
                                                   final String userSpecifiedTemplateResourceName,
                                                   final String overrideParametersStr )
                    throws ChartEngineException
    {
        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = determineChartType( config, input, null );
        String templateName = determineTemplate( input.getMetadata().getMetricID(),
                                                 usedPlotType );
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Setup the default arguments.
        final MetricOutputMetadata meta = input.getMetadata();
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor( input, null );

        //Setup plot specific arguments.
        arguments.addBaselineArguments( meta );
        arguments.addDurationMetricArguments();
        arguments.addPoolingWindowArguments( input );

        //Build the source.
        XYChartDataSource source = null;

        //Setup the assumed source and arguments.
        source = XYChartDataSourceFactory.ofPairedOutputInstantDuration( 0, input );

        //Build the ChartEngine instance.
        return generateChartEngine( Lists.newArrayList( source ),
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    null,
                                    null );
    }


    /**
     * 
     * @param config The project configuration.
     * @param input The input for which to build the categorical plot.
     * @param userSpecifiedTemplateResourceName User specified template resource name providing instructions for display.  
     * If null, then the default template is used.
     * @param overrideParametersStr XML to parse that can then override the template settings.  If null, then parameters
     * are not overridden.
     * @return A {@link ChartEngine} ready to be used to build a chart.
     * @throws ChartEngineException A problem was encountered building the {@link ChartEngine}.
     * @throws XYChartDataSourceException A problem was encountered preparing the categorical source.
     */
    public static ChartEngine
            buildCategoricalDurationScoreChartEngine( final ProjectConfig config,
                                                      MetricOutputMapByTimeAndThreshold<DurationScoreOutput> input,
                                                      final String userSpecifiedTemplateResourceName,
                                                      final String overrideParametersStr )
                    throws ChartEngineException, XYChartDataSourceException
    {
        //Determine the output type, converting DEFAULT accordingly, and template name.
        ChartType usedPlotType = determineChartType( config, input, null );
        String templateName = determineTemplate( input.getMetadata().getMetricID(),
                                                 usedPlotType );
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Setup the default arguments.
        final MetricOutputMetadata meta = input.getMetadata();
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor( input, null );

        //Setup plot specific arguments.
        arguments.addBaselineArguments( meta );
        arguments.addDurationMetricArguments();
        arguments.addPoolingWindowArguments( input );

        //Setup the assumed source and arguments.
        CategoricalXYChartDataSource source =
                XYChartDataSourceFactory.ofDurationScoreCategoricalOutput( 0, input );

        //Build the ChartEngine instance.
        return generateChartEngine( Lists.newArrayList( source ),
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    null,
                                    null );
    }


    /**
     * @param input The pairs to plot.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    public static ChartEngine buildSingleValuedPairsChartEngine( final SingleValuedPairs input,
                                                                 final String userSpecifiedTemplateResourceName,
                                                                 final String overrideParametersStr )
            throws ChartEngineException
    {

        String templateName = "singleValuedPairsTemplate.xml";
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Build the source.
        final DefaultXYChartDataSource source = XYChartDataSourceFactory.ofSingleValuedPairs( 0, input );

        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor( input.getMetadata() );

        //Process override parameters.
        ChartDrawingParameters override = null;
        if ( overrideParametersStr != null )//TRIM ONLY IF NOT NULL!
        {
            final String usedStr = overrideParametersStr.trim();
            if ( !usedStr.isEmpty() )
            {
                override = new ChartDrawingParameters();
                try
                {
                    XMLTools.readXMLFromString( usedStr, override );
                }
                catch ( final Exception t )
                {
                    LOGGER.warn( "Unable to parse XML provided by user for chart drawing: " + t.getMessage() );
                    LOGGER.trace( "Unable to parse XML provided by user for chart drawing", t );
                }
            }
        }

        //Build the ChartEngine instance.
        return ChartTools.buildChartEngine( Lists.newArrayList( source ),
                                            arguments,
                                            templateName,
                                            override );
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
                                                                                      Lists.newArrayList( xValues ),
                                                                                      Lists.newArrayList( yValues ) );
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
     * @param templateName The name of the template system resource or file. It will be loaded here.
     * @param overrideParametersStr The XML string defining the override parameters supplied by a user. They will be
     *            read herein.
     * @param diagonalDataSourceIndices The indices of the data sources used to define the appearance of the diagonals.
     *            ONLY supply these indices if you want the chart engine to add the diagonals through JFreeChart tools;
     *            such diagonals will never appear in the legend, only on the plot. If you need the diagonal to appear
     *            on the legend and on the plot, then you must define it through a regular data source, likely making
     *            use of the constructConnectedPointsDataSource method.
     * @param axisToSquareAgainstDomain A string indicating the axes to square against the domain; either "left" or
     *            "right".
     * @return A {@link WRESChartEngine} instance ready to use.
     * @throws ChartEngineException If the template fails to parse.
     */
    public static WRESChartEngine generateChartEngine( final List<XYChartDataSource> dataSources,
                                                       final ArgumentsProcessor arguments,
                                                       final String templateName,
                                                       final String overrideParametersStr,
                                                       final int[] diagonalDataSourceIndices,
                                                       final String axisToSquareAgainstDomain )
            throws ChartEngineException
    {
        //Load the template parameters.  This will first attempt to load them as a system resource on the class path and
        //then as a file from the file system.  If neither works, it throws an exception.
        final ChartDrawingParameters parameters = new ChartDrawingParameters();
        try
        {
            XMLTools.readXMLFromResource( templateName, parameters );
        }
        catch ( final Exception t )
        {
            try
            {
                XMLTools.readXMLFromFile( new File( templateName ), parameters );
            }
            catch ( final Exception t2 )
            {
                throw new ChartEngineException( "Unable to load default chart drawing parameters from resource or file with name '"
                                                + templateName + "': " + t2.getMessage() );
            }
        }

        //Process override parameters.
        ChartDrawingParameters override = null;
        if ( overrideParametersStr != null )//TRIM ONLY IF NOT NULL!
        {
            final String usedStr = overrideParametersStr.trim();
            if ( !usedStr.isEmpty() )
            {
                override = new ChartDrawingParameters();
                try
                {
                    XMLTools.readXMLFromString( usedStr, override );
                }
                catch ( final Exception t )
                {
                    LOGGER.warn( "Unable to parse XML provided by user for chart drawing: " + t.getMessage() );
                    LOGGER.trace( "Unable to parse XML provided by user for chart drawing", t );
                }
            }
        }

        return new WRESChartEngine( dataSources,
                                    arguments,
                                    parameters,
                                    override,
                                    diagonalDataSourceIndices,
                                    axisToSquareAgainstDomain );
    }
}

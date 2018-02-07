package wres.vis;

import java.awt.geom.Point2D;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.datasource.instances.DataSetXYChartDataSource;
import ohd.hseb.charter.datasource.instances.NumericalXYChartDataSource;
import ohd.hseb.charter.parameters.ChartDrawingParameters;
import ohd.hseb.hefs.utils.arguments.ArgumentsProcessor;
import ohd.hseb.hefs.utils.xml.XMLTools;
import wres.config.generated.PlotTypeSelection;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.outputs.ScoreOutput;

/**
 * Factory to use in order to construct a wres-vis chart.
 * 
 * @author Hank.Herr
 */
public abstract class ChartEngineFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ChartEngineFactory.class );

    /**
     * Maintains information about the different {@link ScoreOutput} plot types, including defaults and expected
     * classes.
     */
    private static EnumMap<PlotTypeSelection, PlotTypeInformation> scalarOutputPlotTypeInfoMap =
            new EnumMap<>( PlotTypeSelection.class );
    static
    {
        scalarOutputPlotTypeInfoMap.put( PlotTypeSelection.LEAD_THRESHOLD,
                                         new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                  ScoreOutput.class,
                                                                  "scalarOutputLeadThresholdTemplate.xml" ) );
        scalarOutputPlotTypeInfoMap.put( PlotTypeSelection.THRESHOLD_LEAD,
                                         new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                  ScoreOutput.class,
                                                                  "scalarOutputThresholdLeadTemplate.xml" ) );
        scalarOutputPlotTypeInfoMap.put( PlotTypeSelection.POOLING_WINDOW,
                                         new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                  ScoreOutput.class,
                                                                  "scalarOutputPoolingWindowTemplate.xml" ) );
    }
    
    /**
     * Maintains information about the different {@link ScoreOutput} plot types, including defaults and expected
     * classes.
     */
    private static EnumMap<PlotTypeSelection, PlotTypeInformation> pairedInstantDurationOutputPlotTypeInfoMap =
            new EnumMap<>( PlotTypeSelection.class );
    static
    {
        pairedInstantDurationOutputPlotTypeInfoMap.put( PlotTypeSelection.POOLING_WINDOW,
                                         new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                  PairedOutput.class,
                                                                  "timeToPeakErrorTemplate.xml" ) );
    }

    /**
     * Maintains information about the different {@link MultiVectorOutput} plot types, including defaults and expected
     * classes.  For metrics where the plot type is not used, such as box plots, you must put the information using
     * a {@link PlotTypeSelection#LEAD_THRESHOLD}!  You cannot "put" with a null plot type selection!
     */
    private static Table<MetricConstants, PlotTypeSelection, PlotTypeInformation> multiVectorOutputPlotTypeInfoTable =
            HashBasedTable.create();
    static
    {
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.RELIABILITY_DIAGRAM,
                                                PlotTypeSelection.LEAD_THRESHOLD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "reliabilityDiagramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.RELIABILITY_DIAGRAM,
                                                PlotTypeSelection.THRESHOLD_LEAD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "reliabilityDiagramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                PlotTypeSelection.LEAD_THRESHOLD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "rocDiagramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                PlotTypeSelection.THRESHOLD_LEAD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "rocDiagramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                                PlotTypeSelection.LEAD_THRESHOLD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "qqDiagramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                                PlotTypeSelection.THRESHOLD_LEAD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "qqDiagramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.RANK_HISTOGRAM,
                                                PlotTypeSelection.LEAD_THRESHOLD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "rankHistogramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.RANK_HISTOGRAM,
                                                PlotTypeSelection.THRESHOLD_LEAD,
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         MultiVectorOutput.class,
                                                                         "rankHistogramTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                                PlotTypeSelection.LEAD_THRESHOLD, //Unimportant
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         BoxPlotOutput.class,
                                                                         "boxPlotOfErrorsTemplate.xml" ) );
        multiVectorOutputPlotTypeInfoTable.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                PlotTypeSelection.LEAD_THRESHOLD, //Unimportant
                                                new PlotTypeInformation( MetricOutputMapByTimeAndThreshold.class,
                                                                         BoxPlotOutput.class,
                                                                         "boxPlotOfErrorsTemplate.xml" ) );
    }


    /**
     * Per SonarLint, hiding the public constructor.
     */
    private ChartEngineFactory()
    {
    }

    /**
     * @param metricId the metric identifier
     * @param plotType the plot type.  May be null.  If it is null, {@link PlotTypeSelection#LEAD_THRESHOLD} is used to access
     *        the map of information; see the map comment for more info.
     * @return {@link PlotTypeInformation} for the provided {@link MetricConstants} metric id and
     *         {@link PlotTypeSelection}. This will throw an {@link IllegalArgumentException} if the combination is not
     *         yet supported in the {@link #multiVectorOutputPlotTypeInfoTable}.
     */
    public static PlotTypeInformation getNonNullMultiVectorOutputPlotTypeInformation( final MetricConstants metricId,
                                                                                      final PlotTypeSelection plotType )
    {
        PlotTypeSelection usedPlotType = plotType;
        if ( plotType == null )
        {
            usedPlotType = PlotTypeSelection.LEAD_THRESHOLD;
        }

        final PlotTypeInformation results = multiVectorOutputPlotTypeInfoTable.get( metricId, usedPlotType );
        if ( results == null )
        {
            throw new IllegalArgumentException( "MultiVectorOutput plot type for metric " + metricId
                                                + " and plot type "
                                                + plotType
                                                + " is not supported." );
        }
        return results;
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
                                       PlotTypeSelection usedPlotType,
                                       String templateName,
                                       String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        WRESArgumentProcessor arguments = null;
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice;

        //-----------------------------------------------------------------
        //Reliability diagram for each lead time, thresholds in the legend.
        //-----------------------------------------------------------------
        if ( usedPlotType.equals( PlotTypeSelection.LEAD_THRESHOLD ) )
        {

            inputSlice = input.filterByTime( (TimeWindow) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addLeadThresholdArguments( inputSlice, (TimeWindow) inputKeyInstance );
        }

        //-----------------------------------------------------------------
        //Reliability diagram for each threshold, lead times in the legend.
        //-----------------------------------------------------------------
        else if ( usedPlotType.equals( PlotTypeSelection.THRESHOLD_LEAD ) )
        {
            inputSlice =
                    input.filterByThreshold( (Threshold) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addThresholdLeadArguments( inputSlice, (Threshold) inputKeyInstance );
        }

        //This is an error, since there are only two allowable types of reliability diagrams.
        else
        {
            throw new IllegalArgumentException( "Plot type " + usedPlotType
                                                + " is invalid for a reliability diagram." );
        }


        dataSources.add( new MultiVectorOutputDiagramXYChartDataSource( 0,
                                                                        inputSlice,
                                                                        MetricDimension.FORECAST_PROBABILITY,
                                                                        MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                                        MetricDimension.FORECAST_PROBABILITY.toString(),
                                                                        MetricDimension.OBSERVED_RELATIVE_FREQUENCY.toString() ) );
        dataSources.add( new MultiVectorOutputDiagramXYChartDataSource( 1,
                                                                        inputSlice,
                                                                        MetricDimension.FORECAST_PROBABILITY,
                                                                        MetricDimension.SAMPLE_SIZE,
                                                                        MetricDimension.FORECAST_PROBABILITY.toString(),
                                                                        MetricDimension.SAMPLE_SIZE.toString(),
                                                                        1 ) );
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
                               PlotTypeSelection usedPlotType,
                               String templateName,
                               String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        WRESArgumentProcessor arguments = null;
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;
        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice;

        //-----------------------------------------------------------------
        //ROC diagram for each lead time, thresholds in the legend.
        //-----------------------------------------------------------------
        if ( usedPlotType.equals( PlotTypeSelection.LEAD_THRESHOLD ) )
        {
            inputSlice =
                    input.filterByTime( (TimeWindow) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addLeadThresholdArguments( inputSlice, (TimeWindow) inputKeyInstance );
        }

        //-----------------------------------------------------------------
        //ROC diagram for each threshold, lead times in the legend.
        //-----------------------------------------------------------------
        else if ( usedPlotType.equals( PlotTypeSelection.THRESHOLD_LEAD ) )
        {
            inputSlice =
                    input.filterByThreshold( (Threshold) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addThresholdLeadArguments( inputSlice, (Threshold) inputKeyInstance );
        }

        //This is an error, since there are only two allowable types of reliability diagrams.
        else
        {
            throw new IllegalArgumentException( "Plot type " + usedPlotType
                                                + " is invalid for a ROC diagram." );
        }

        dataSources.add( new MultiVectorOutputDiagramXYChartDataSource( 0,
                                                                        inputSlice,
                                                                        MetricDimension.PROBABILITY_OF_FALSE_DETECTION,
                                                                        MetricDimension.PROBABILITY_OF_DETECTION,
                                                                        MetricDimension.PROBABILITY_OF_FALSE_DETECTION.toString(),
                                                                        MetricDimension.PROBABILITY_OF_DETECTION.toString() ) );
        //Diagonal data source added so that it shows up in the legend.
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
                              PlotTypeSelection usedPlotType,
                              String templateName,
                              String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        WRESArgumentProcessor arguments = null;
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;
        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice;

        //-----------------------------------------------------------------
        //QQ diagram for each lead time, thresholds in the legend.
        //-----------------------------------------------------------------
        if ( usedPlotType.equals( PlotTypeSelection.LEAD_THRESHOLD ) )
        {
            inputSlice =
                    input.filterByTime( (TimeWindow) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addLeadThresholdArguments( inputSlice, (TimeWindow) inputKeyInstance );
        }

        //-----------------------------------------------------------------
        //QQ diagram for each threshold, lead times in the legend.
        //-----------------------------------------------------------------
        else if ( usedPlotType.equals( PlotTypeSelection.THRESHOLD_LEAD ) )
        {
            inputSlice =
                    input.filterByThreshold( (Threshold) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addThresholdLeadArguments( inputSlice, (Threshold) inputKeyInstance );
        }

        //This is an error, since there are only two allowable types of reliability diagrams.
        else
        {
            throw new IllegalArgumentException( "Plot type " + usedPlotType
                                                + " is invalid for a QQ diagram." );
        }

        final MultiVectorOutputDiagramXYChartDataSource dataSource =
                new MultiVectorOutputDiagramXYChartDataSource( 0,
                                                               inputSlice,
                                                               MetricDimension.OBSERVED_QUANTILES,
                                                               MetricDimension.PREDICTED_QUANTILES,
                                                               MetricConstants.MetricDimension.OBSERVED_QUANTILES.toString()
                                                                                                    + " @variableName@@inputUnitsLabelSuffix@",
                                                               MetricConstants.MetricDimension.PREDICTED_QUANTILES.toString() + " @variableName@@inputUnitsLabelSuffix@" );
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
                                  PlotTypeSelection usedPlotType,
                                  String templateName,
                                  String overrideParametersStr )
                    throws ChartEngineException
    {
        final List<XYChartDataSource> dataSources = new ArrayList<>();
        WRESArgumentProcessor arguments = null;
        int[] diagonalDataSourceIndices = null;
        String axisToSquareAgainstDomain = null;
        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> inputSlice;

        //-----------------------------------------------------------------
        //Rank Histogram diagram for each lead time, thresholds in the legend.
        //-----------------------------------------------------------------
        if ( usedPlotType.equals( PlotTypeSelection.LEAD_THRESHOLD ) )
        {
            inputSlice =
                    input.filterByTime( (TimeWindow) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addLeadThresholdArguments( inputSlice, (TimeWindow) inputKeyInstance );
        }

        //-----------------------------------------------------------------
        //Rank histogram diagram for each threshold, lead times in the legend.
        //-----------------------------------------------------------------
        else if ( usedPlotType.equals( PlotTypeSelection.THRESHOLD_LEAD ) )
        {
            inputSlice =
                    input.filterByThreshold( (Threshold) inputKeyInstance );

            //Setup the default arguments.
            arguments = new WRESArgumentProcessor( inputSlice, usedPlotType );
            arguments.addThresholdLeadArguments( inputSlice, (Threshold) inputKeyInstance );
        }

        //This is an error, since there are only two allowable types of reliability diagrams.
        else
        {
            throw new IllegalArgumentException( "Plot type " + usedPlotType
                                                + " is invalid for a rank histogram." );
        }

        final MultiVectorOutputDiagramXYChartDataSource dataSource =
                new MultiVectorOutputDiagramXYChartDataSource( 0,
                                                               inputSlice,
                                                               MetricDimension.RANK_ORDER,
                                                               MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                               "Bin Separating Ranked Eensemble Members",
                                                               MetricDimension.OBSERVED_RELATIVE_FREQUENCY.toString() )
                {
                    @Override
                    protected
                            MultiVectorOutputDiagramXYDataset
                            instantiateXYDataset()
                    {
                        return new RankHistogramXYDataset( getInput(),
                                                           getXConstant(),
                                                           getYConstant() );
                    }
                };
        dataSources.add( dataSource );

        //Build the ChartEngine instance.
        return generateChartEngine( dataSources,
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    diagonalDataSourceIndices,
                                    axisToSquareAgainstDomain );
    }

    /**Calls the process methods as appropriate for the given plot type.
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
            buildMultiVectorOutputChartEngine( final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                                               final DataFactory factory,
                                               final PlotTypeSelection userSpecifiedPlotType,
                                               final String userSpecifiedTemplateResourceName,
                                               final String overrideParametersStr )
                    throws ChartEngineException
    {
        final ConcurrentMap<Object, ChartEngine> results = new ConcurrentSkipListMap<>();

        //Determine used plot type and template name.  Note that if no plot type information is provided for the metric id
        //and plot type, then an illegal argument exception will be thrown.
        PlotTypeSelection usedPlotType = PlotTypeSelection.LEAD_THRESHOLD; //Lead time first plot type is the default!!!
        if ( userSpecifiedPlotType != null )
        {
            usedPlotType = userSpecifiedPlotType;
        }
        String templateName =
                getNonNullMultiVectorOutputPlotTypeInformation( input.getMetadata().getMetricID(),
                                                                usedPlotType ).getDefaultTemplateName();
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Determine the key set for the loop below based on if this is a lead time first and threshold first plot type.
        Set<?> keySetValues = input.keySetByTime();
        if ( usedPlotType.equals( PlotTypeSelection.THRESHOLD_LEAD ) )
        {
            keySetValues = input.keySetByThreshold();
        }

        //For each lead time, do the following....
        for ( final Object keyInstance : keySetValues )
        {
            if ( input.getMetadata().getMetricID() == MetricConstants.RELIABILITY_DIAGRAM )
            {

                final ChartEngine engine =
                        processReliabilityDiagram( keyInstance,
                                                   input,
                                                   factory,
                                                   usedPlotType,
                                                   templateName,
                                                   overrideParametersStr );
                results.put( keyInstance, engine );
            }
            else if ( input.getMetadata().getMetricID() == MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
            {

                final ChartEngine engine =
                        processROCDiagram( keyInstance,
                                           input,
                                           factory,
                                           usedPlotType,
                                           templateName,
                                           overrideParametersStr );
                results.put( keyInstance, engine );
            }
            else if ( input.getMetadata().getMetricID() == MetricConstants.QUANTILE_QUANTILE_DIAGRAM )
            {

                final ChartEngine engine =
                        processQQDiagram( keyInstance,
                                          input,
                                          factory,
                                          usedPlotType,
                                          templateName,
                                          overrideParametersStr );
                results.put( keyInstance, engine );
            }
            else if ( input.getMetadata().getMetricID() == MetricConstants.RANK_HISTOGRAM )
            {

                final ChartEngine engine =
                        processRankHistogram( keyInstance,
                                              input,
                                              factory,
                                              usedPlotType,
                                              templateName,
                                              overrideParametersStr );
                results.put( keyInstance, engine );
            }

            //===================================================
            //Unrecognized plot type in metrics.
            //===================================================
            else
            {
                throw new IllegalArgumentException( "Unrecognized plot type of " + input.getMetadata().getMetricID()
                                                    + " specified in the metric information." );
            }
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
                                         Pair<TimeWindow, Threshold> inputKeyInstance,
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
        dataSources.add( new BoxPlotDiagramXYChartDataSource( 0, boxPlotData ) );

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
     * @param input The metric output to plot.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return Map where the keys are instances of {@link Pair} with the two keys being an integer and a threshold.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     */
    public static ConcurrentMap<Pair<TimeWindow, Threshold>, ChartEngine>
            buildBoxPlotChartEngine( final MetricOutputMapByTimeAndThreshold<BoxPlotOutput> input,
                                     final String userSpecifiedTemplateResourceName,
                                     final String overrideParametersStr )
                    throws ChartEngineException
    {
        final ConcurrentMap<Pair<TimeWindow, Threshold>, ChartEngine> results = new ConcurrentSkipListMap<>();

        String templateName =
                getNonNullMultiVectorOutputPlotTypeInformation( input.getMetadata().getMetricID(),
                                                                null ).getDefaultTemplateName();
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Determine the key set for the loop below based on if this is a lead time first and threshold first plot type.
        Set<Pair<TimeWindow, Threshold>> keySetValues = input.keySet();

        //For each lead time, do the following....
        for ( final Pair<TimeWindow, Threshold> keyInstance : keySetValues )
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
            //Unrecognized plot type in metrics.
            //===================================================
            else
            {
                throw new IllegalArgumentException( "Unrecognized plot type of " + input.getMetadata().getMetricID()
                                                    + " specified in the metric information." );
            }
        }
        return results;
    }


    /**
     * Builds a {@link ChartEngine} for each component of a score.
     * 
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
            buildScoreOutputChartEngine( final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input,
                                         final DataFactory factory,
                                         final PlotTypeSelection userSpecifiedPlotType,
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
            final ChartEngine engine = buildScoreOutputChartEngine( entry.getValue(),
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
            buildScoreOutputChartEngine( final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input,
                                         final PlotTypeSelection userSpecifiedPlotType,
                                         final String userSpecifiedTemplateResourceName,
                                         final String overrideParametersStr )
                    throws ChartEngineException
    {
        //Define the used plot type.
        PlotTypeSelection usedPlotType = PlotTypeSelection.LEAD_THRESHOLD;
        if ( userSpecifiedPlotType != null )
        {
            usedPlotType = userSpecifiedPlotType;
        }

        //Setup the default arguments.
        final MetricOutputMetadata meta = input.getMetadata();
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor( input, usedPlotType );

        //Setup plot specific arguments.
        arguments.addBaselineArguments( meta );

        //Build the source.
        XYChartDataSource source = null;
        String templateName = scalarOutputPlotTypeInfoMap.get( usedPlotType ).getDefaultTemplateName();
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Lead-threshold is the default.  This is for plots with the lead time on the domain axis and threshold in the legend.
        if ( usedPlotType.equals( PlotTypeSelection.LEAD_THRESHOLD ) )
        {
            source = new ScoreOutputByLeadAndThresholdXYChartDataSource( 0, input );
            arguments.addLeadThresholdArguments( input, null );
        }
        //This is for plots with the threshold on the domain axis and lead time in the legend.
        else if ( usedPlotType.equals( PlotTypeSelection.THRESHOLD_LEAD ) )
        {
            source = new ScoreOutputByThresholdAndLeadXYChartDataSource( 0, input );
            arguments.addThresholdLeadArguments( input, null );
        }
        //This is for plots that operate with sequences of time windows (e.g. rolling windows)
        else if ( usedPlotType.equals( PlotTypeSelection.POOLING_WINDOW ) )
        {
            source = new ScoreOutputByPoolingWindowXYChartDataSource( 0, input );
            arguments.addPoolingWindowArguments( input );
        }
        else
        {
            throw new IllegalArgumentException( "Plot type of " + userSpecifiedPlotType
                                                + " is not valid for a generic scalar output plot by lead/threshold." );
        }

        //Build the ChartEngine instance.
        return generateChartEngine( Lists.newArrayList( source ),
                                    arguments,
                                    templateName,
                                    overrideParametersStr,
                                    null,
                                    null );
    }


    public static ChartEngine
            buildPairedInstantDurationChartEngine( MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> input,
                                                   final String userSpecifiedTemplateResourceName,
                                                   final String overrideParametersStr )
                    throws ChartEngineException
    {
        //Setup the default arguments.
        final MetricOutputMetadata meta = input.getMetadata();
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor( input, null );

        //Setup plot specific arguments.
        arguments.addBaselineArguments( meta );
        arguments.addDurationMetricArguments();

        //Build the source.  Note that the POOLING_WINDOW is used below as filler.  Once we have a real plot type
        //we can update the map at the top and use the appropriate plot type here.
        XYChartDataSource source = null;
        String templateName = pairedInstantDurationOutputPlotTypeInfoMap.get(  PlotTypeSelection.POOLING_WINDOW ).getDefaultTemplateName();
        if ( userSpecifiedTemplateResourceName != null )
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Setup the assumed source and arguments.
        source = new DurationScoreOutputByBasisTimeXYChartDataSource( 0, input );
        arguments.addPoolingWindowArguments( input );

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
        final SingleValuedPairsXYChartDataSource source = new SingleValuedPairsXYChartDataSource( 0, input );

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

    /**
     * Stores information about each plot type necessary to validation user parameters, including the default template
     * name.
     * 
     * @author hank.herr
     */
    public static class PlotTypeInformation
    {
        private final Class<?> expectedPlotDataClass;
        private final Class<?> dataGenericType;
        private final String defaultTemplateName;

        public PlotTypeInformation( final Class<?> expectedPlotDataClass,
                                    final Class<?> dataGenericType,
                                    final String defaultTemplateName )
        {
            this.expectedPlotDataClass = expectedPlotDataClass;
            this.dataGenericType = dataGenericType;
            this.defaultTemplateName = defaultTemplateName;
        }

        public Class<?> getExpectedPlotDataClass()
        {
            return expectedPlotDataClass;
        }

        public Class<?> getDataGenericType()
        {
            return dataGenericType;
        }

        public String getDefaultTemplateName()
        {
            return defaultTemplateName;
        }
    }
}

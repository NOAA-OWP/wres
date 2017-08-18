package wres.vis;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.datasource.instances.DataSetXYChartDataSource;
import ohd.hseb.charter.datasource.instances.NumericalXYChartDataSource;
import ohd.hseb.charter.parameters.ChartDrawingParameters;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import ohd.hseb.hefs.utils.xml.XMLTools;
import wres.config.generated.PlotTypeSelection;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DatasetIdentifier;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Threshold;
import wres.datamodel.metric.VectorOutput;

/**
 * Factory to use in order to construct a wres-vis chart.
 * 
 * @author Hank.Herr
 */
public abstract class ChartEngineFactory
{

    /**
     * Maintains information about the different {@link ScalarOutput} plot types, including defaults and expected
     * classes.
     */
    private static HashMap<PlotTypeSelection, PlotTypeInformation> scalarOutputPlotTypeInfoMap = new HashMap<>();
    static
    {
        scalarOutputPlotTypeInfoMap.put(PlotTypeSelection.LEAD_THRESHOLD,
                                        new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                ScalarOutput.class,
                                                                "scalarOutputLeadThresholdTemplate.xml"));
        scalarOutputPlotTypeInfoMap.put(PlotTypeSelection.THRESHOLD_LEAD,
                                        new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                ScalarOutput.class,
                                                                "scalarOutputThresholdLeadTemplate.xml"));
    }

    /**
     * Maintains information about the different {@link MultiVectorOutput} plot types, including defaults and expected
     * classes.
     */
    private static Table<MetricConstants, PlotTypeSelection, PlotTypeInformation> multiVectorOutputPlotTypeInfoTable =
                                                                                                                   HashBasedTable.create();
    static
    {
        multiVectorOutputPlotTypeInfoTable.put(MetricConstants.RELIABILITY_DIAGRAM,
                                             PlotTypeSelection.LEAD_THRESHOLD,
                                             new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                     MultiVectorOutput.class,
                                                                     "reliabilityDiagramTemplate.xml"));
        multiVectorOutputPlotTypeInfoTable.put(MetricConstants.RELIABILITY_DIAGRAM,
                                             PlotTypeSelection.THRESHOLD_LEAD,
                                             new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                     MultiVectorOutput.class,
                                                                     "reliabilityDiagramTemplate.xml"));
        multiVectorOutputPlotTypeInfoTable.put(MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                             PlotTypeSelection.LEAD_THRESHOLD,
                                             new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                     MultiVectorOutput.class,
                                                                     "rocDiagramTemplate.xml"));
        multiVectorOutputPlotTypeInfoTable.put(MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                             PlotTypeSelection.THRESHOLD_LEAD,
                                             new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                     MultiVectorOutput.class,
                                                                     "rocDiagramTemplate.xml"));
        multiVectorOutputPlotTypeInfoTable.put(MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                             PlotTypeSelection.LEAD_THRESHOLD,
                                             new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                     MultiVectorOutput.class,
                                                                     "qqDiagramTemplate.xml"));
        multiVectorOutputPlotTypeInfoTable.put(MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                             PlotTypeSelection.THRESHOLD_LEAD,
                                             new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                                     MultiVectorOutput.class,
                                                                     "qqDiagramTemplate.xml"));
    }

    /**
     * @return {@link PlotTypeInformation} for the provided {@link MetricConstants} metric id and
     *         {@link PlotTypeSelection}. This will throw an {@link IllegalArgumentException} if the combination is not
     *         yet supported in the {@link #multiVectorOutputPlotTypeInfoTable}.
     */
    public static PlotTypeInformation getNonNullMultiVectorOutputPlotTypeInformation(final MetricConstants metricId,
                                                                                     final PlotTypeSelection plotType)
    {
        final PlotTypeInformation results = multiVectorOutputPlotTypeInfoTable.get(metricId, plotType);
        if(results == null)
        {
            throw new IllegalArgumentException("MultiVectorOutput plot type for metric " + metricId + " and plot type "
                + plotType + " is not supported.");
        }
        return results;
    }

    /**
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param userSpecifiedPlotType An optional plot type to generate, where multiple plot types are supported for the
     *            input. May be null.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static table.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     * @throws GenericXMLReadingHandlerException If the override XML cannot be parsed.
     */
    public static ConcurrentMap<Object, ChartEngine> buildMultiVectorOutputChartEngine(final MetricOutputMapByLeadThreshold<MultiVectorOutput> input,
                                                                                       final DataFactory factory,
                                                                                       final PlotTypeSelection userSpecifiedPlotType,
                                                                                       final String userSpecifiedTemplateResourceName,
                                                                                       final String overrideParametersStr) throws ChartEngineException,
                                                                                                                           GenericXMLReadingHandlerException
    {
        final ConcurrentMap<Object, ChartEngine> results = new ConcurrentSkipListMap<>();

        //Determine used plot type and template name.  Note that if no plot type information is provided for the metric id
        //and plot type, then an illegal argument exception will be thrown.
        PlotTypeSelection usedPlotType = PlotTypeSelection.LEAD_THRESHOLD; //Lead time first plot type is the default!!!
        if(userSpecifiedPlotType != null)
        {
            usedPlotType = userSpecifiedPlotType;
        }
        final String templateName =
                                  getNonNullMultiVectorOutputPlotTypeInformation(input.getMetadata().getMetricID(),
                                                                                 usedPlotType).getDefaultTemplateName();

        //Determine the key set for the loop below based on if this is a lead time first and threshold first plot type.
        Set<?> keySetValues = input.keySetByLead();
        if(usedPlotType.equals(PlotTypeSelection.THRESHOLD_LEAD))
        {
            keySetValues = input.keySetByThreshold();
        }

        //For each lead time, do the following....
        for(final Object keyInstance: keySetValues)
        {
            final List<XYChartDataSource> dataSources = new ArrayList<>();
            WRESArgumentProcessor arguments = null;

            //=====================
            //RELIABILITY DIAGRAM
            //=====================
            if(input.getMetadata().getMetricID() == MetricConstants.RELIABILITY_DIAGRAM)
            {
                //-----------------------------------------------------------------
                //Reliability diagram for each lead time, thresholds in the legend.
                //-----------------------------------------------------------------
                if((userSpecifiedPlotType == null) || (userSpecifiedPlotType.equals(PlotTypeSelection.LEAD_THRESHOLD)))
                {
                    final MetricOutputMapByLeadThreshold<MultiVectorOutput> inputSlice;
                    inputSlice = input.sliceByLead((Integer)keyInstance);

                    //Setup the default arguments.
                    final MetricOutputMetadata meta = inputSlice.getMetadata();
                    arguments = buildDefaultMetricOutputPlotArgumentsProcessor(factory, meta);

                    //Legend title and lead time argument are specific to the plot.
                    final String legendTitle = "Threshold";
                    String legendUnitsText = "";
                    if(input.hasQuantileThresholds())
                    {
                        legendUnitsText += " [" + meta.getInputDimension() + "]";
                    }
                    else if(input.keySetByThreshold().size() > 1)
                    {
                        legendUnitsText += " [" + meta.getInputDimension() + "]";
                    }
                    arguments.addArgument("legendTitle", legendTitle);
                    arguments.addArgument("legendUnitsText", legendUnitsText);
                    arguments.addArgument("diagramInstanceDescription",
                                          "at Lead Hour " + keyInstance);
                    arguments.addArgument("plotTitleVariable", "Lead Times");

                    dataSources.add(new MultiVectorOutputDiagramXYChartDataSource(0,
                                                                                  inputSlice,
                                                                                  MetricConstants.FORECAST_PROBABILITY,
                                                                                  MetricConstants.OBSERVED_GIVEN_FORECAST_PROBABILITY,
                                                                                  "Forecast Probability",
                                                                                  "Observed Probability Given Forecast Probability"));
                    dataSources.add(new MultiVectorOutputDiagramXYChartDataSource(1,
                                                                                  inputSlice,
                                                                                  MetricConstants.FORECAST_PROBABILITY,
                                                                                  MetricConstants.SAMPLE_SIZE,
                                                                                  "Forecast Probability",
                                                                                  "Samples"));
                    dataSources.add(constructConnectedPointsDataSource(2,
                                                                       new Point2D.Double(0.0, 0.0),
                                                                       new Point2D.Double(1.0, 1.0)));
                }

                //-----------------------------------------------------------------
                //Reliability diagram for each treshold, lead times in the legend.
                //-----------------------------------------------------------------
                else if(userSpecifiedPlotType.equals(PlotTypeSelection.THRESHOLD_LEAD))
                {
                    final MetricOutputMapByLeadThreshold<MultiVectorOutput> inputSlice =
                                                                                       input.sliceByThreshold((Threshold)keyInstance);

                    //Setup the default arguments.
                    final MetricOutputMetadata meta = inputSlice.getMetadata();
                    arguments = buildDefaultMetricOutputPlotArgumentsProcessor(factory, meta);

                    //Legend title and lead time argument are specific to the plot.
                    arguments.addArgument("legendTitle", "Lead Time");
                    arguments.addArgument("legendUnitsText", " [hours]");
                    arguments.addArgument("diagramInstanceDescription",
                                          "for Threshold " + ((Threshold)keyInstance).toString() + " ("
                                              + meta.getInputDimension() + ")");
                    arguments.addArgument("plotTitleVariable", "Thresholds");

                    dataSources.add(new MultiVectorOutputDiagramXYChartDataSource(0,
                                                                                  inputSlice,
                                                                                  MetricConstants.FORECAST_PROBABILITY,
                                                                                  MetricConstants.OBSERVED_GIVEN_FORECAST_PROBABILITY,
                                                                                  "Forecast Probability",
                                                                                  "Observed Probability Given Forecast Probability"));
                    dataSources.add(new MultiVectorOutputDiagramXYChartDataSource(1,
                                                                                  inputSlice,
                                                                                  MetricConstants.FORECAST_PROBABILITY,
                                                                                  MetricConstants.SAMPLE_SIZE,
                                                                                  "Forecast Probability",
                                                                                  "Samples"));
                    dataSources.add(constructConnectedPointsDataSource(2,
                                                                       new Point2D.Double(0.0, 0.0),
                                                                       new Point2D.Double(1.0, 1.0)));
                }

                //This is an error, since there are only two allowable types of reliability diagrams.
                else
                {
                    throw new IllegalArgumentException("Plot type " + userSpecifiedPlotType
                        + " is invalid for a reliability diagram.");
                }
            }

            //=====================
            //ROC DIAGRAM
            //=====================
            else if(input.getMetadata().getMetricID() == MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM)
            {
                //-----------------------------------------------------------------
                //ROC diagram for each lead time, thresholds in the legend.
                //-----------------------------------------------------------------
                if((userSpecifiedPlotType == null) || userSpecifiedPlotType.equals(PlotTypeSelection.LEAD_THRESHOLD))
                {
                    final MetricOutputMapByLeadThreshold<MultiVectorOutput> inputSlice =
                                                                                       input.sliceByLead((Integer)keyInstance);

                    //Setup the default arguments.
                    final MetricOutputMetadata meta = inputSlice.getMetadata();
                    arguments = buildDefaultMetricOutputPlotArgumentsProcessor(factory, meta);

                    //Legend title and lead time argument are specific to the plot.
                    final String legendTitle = "Threshold";
                    String legendUnitsText = "";
                    if(input.hasQuantileThresholds())
                    {
                        legendUnitsText += " [" + meta.getInputDimension() + "]";
                    }
                    else if(input.keySetByThreshold().size() > 1)
                    {
                        legendUnitsText += " [" + meta.getInputDimension() + "]";
                    }
                    arguments.addArgument("legendTitle", legendTitle);
                    arguments.addArgument("legendUnitsText", legendUnitsText);
                    arguments.addArgument("diagramInstanceDescription",
                                          "at Lead Hour " + inputSlice.getKey(0).getFirstKey().toString());
                    arguments.addArgument("plotTitleVariable", "Lead Times");

                    dataSources.add(new MultiVectorOutputDiagramXYChartDataSource(0,
                                                                                  inputSlice,
                                                                                  MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                                                                  MetricConstants.PROBABILITY_OF_DETECTION,
                                                                                  "Probability of False Detection",
                                                                                  "Probability of Detection"));
                    dataSources.add(constructConnectedPointsDataSource(1,
                                                                       new Point2D.Double(0.0, 0.0),
                                                                       new Point2D.Double(1.0, 1.0)));
                }

                //-----------------------------------------------------------------
                //ROC diagram for each threshold, lead times in the legend.
                //-----------------------------------------------------------------
                else if(userSpecifiedPlotType.equals(PlotTypeSelection.THRESHOLD_LEAD))
                {
                    final MetricOutputMapByLeadThreshold<MultiVectorOutput> inputSlice =
                                                                                       input.sliceByThreshold((Threshold)keyInstance);

                    //Setup the default arguments.
                    final MetricOutputMetadata meta = inputSlice.getMetadata();
                    arguments = buildDefaultMetricOutputPlotArgumentsProcessor(factory, meta);

                    //Legend title and lead time argument are specific to the plot.
                    arguments.addArgument("legendTitle", "Lead Time");
                    arguments.addArgument("legendUnitsText", " [hours]");
                    arguments.addArgument("leadHour", inputSlice.getKey(0).getFirstKey().toString());
                    arguments.addArgument("diagramInstanceDescription",
                                          "for Threshold " + ((Threshold)keyInstance).toString() + " ("
                                              + meta.getInputDimension() + ")");
                    arguments.addArgument("plotTitleVariable", "Thresholds");

                    dataSources.add(new MultiVectorOutputDiagramXYChartDataSource(0,
                                                                                  inputSlice,
                                                                                  MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                                                                  MetricConstants.PROBABILITY_OF_DETECTION,
                                                                                  "Probability of False Detection",
                                                                                  "Probability of Detection"));
                    dataSources.add(constructConnectedPointsDataSource(1,
                                                                       new Point2D.Double(0.0, 0.0),
                                                                       new Point2D.Double(1.0, 1.0)));
                }

                //This is an error, since there are only two allowable types of reliability diagrams.
                else
                {
                    throw new IllegalArgumentException("Plot type " + userSpecifiedPlotType
                        + " is invalid for a ROC diagram.");
                }
            }

            //===================================================
            //Unrecognized plot type in metrics.
            //===================================================
            else
            {
                throw new IllegalArgumentException("Unrecognized plot type of " + input.getMetadata().getMetricID()
                    + " specified in the metric information.");
            }

            //Process override parameters.
            ChartDrawingParameters override = null;
            if(overrideParametersStr != null)//TRIM ONLY IF NOT NULL!
            {
                final String usedStr = overrideParametersStr.trim();
                if(!usedStr.isEmpty())
                {
                    override = new ChartDrawingParameters();
                    XMLTools.readXMLFromString(usedStr, override);
                }
            }

            //Build the ChartEngine instance.
            final ChartEngine engine = ChartTools.buildChartEngine(dataSources, arguments, templateName, override);
            results.put(keyInstance, engine);
        }
        return results;
    }

    /**
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param userSpecifiedPlotType An optional plot type to generate, where multiple plot types are supported for the
     *            input. May be null.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static maps.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return
     * @throws ChartEngineException
     * @throws GenericXMLReadingHandlerException
     */
    public static ConcurrentMap<Object, ChartEngine> buildVectorOutputChartEngine(final MetricOutputMapByLeadThreshold<VectorOutput> input,
                                                                                  final DataFactory factory,
                                                                                  final PlotTypeSelection userSpecifiedPlotType,
                                                                                  final String userSpecifiedTemplateResourceName,
                                                                                  final String overrideParametersStr) throws ChartEngineException,
                                                                                                                      GenericXMLReadingHandlerException
    {
        final ConcurrentMap<Object, ChartEngine> results = new ConcurrentSkipListMap<>();

        final Map<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> slicedInput =
                                                                                             factory.getSlicer()
                                                                                                    .sliceByMetricComponent(input);
        for(final Map.Entry<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> entry: slicedInput.entrySet())
        {
            final ChartEngine engine = buildGenericScalarOutputChartEngine(entry.getValue(),
                                                                           factory,
                                                                           userSpecifiedPlotType,
                                                                           userSpecifiedTemplateResourceName,
                                                                           overrideParametersStr);
            results.put(entry.getKey(), engine);
        }
        return results;
    }

    /**
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param userSpecifiedPlotType The plot type to generate. For this chart, the plot type must be either
     *            {@link VisualizationPlotType#LEAD_THRESHOLD} or {@link VisualizationPlotType#THRESHOLD_LEAD}. May be
     *            null and, if so, defaults to {@link VisualizationPlotType#LEAD_THRESHOLD}.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in static map/table.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     * @throws GenericXMLReadingHandlerException If the override XML cannot be parsed.
     */
    public static ChartEngine buildGenericScalarOutputChartEngine(final MetricOutputMapByLeadThreshold<ScalarOutput> input,
                                                                  final DataFactory factory,
                                                                  final PlotTypeSelection userSpecifiedPlotType,
                                                                  final String userSpecifiedTemplateResourceName,
                                                                  final String overrideParametersStr) throws ChartEngineException,
                                                                                                      GenericXMLReadingHandlerException
    {
        //Define the used plot type.
        PlotTypeSelection usedPlotType = PlotTypeSelection.LEAD_THRESHOLD;
        if(userSpecifiedPlotType != null)
        {
            usedPlotType = userSpecifiedPlotType;
        }

        //Setup the default arguments.
        final MetricOutputMetadata meta = input.getMetadata();
        final WRESArgumentProcessor arguments = buildDefaultMetricOutputPlotArgumentsProcessor(factory, meta);

        //Setup plot specific arguments.
        final DatasetIdentifier identifier = meta.getIdentifier();
        String baselineText = "";
        if(!Objects.isNull(identifier.getScenarioIDForBaseline()))
        {
            baselineText = " Against Predictions From " + identifier.getScenarioIDForBaseline();
        }
        arguments.addArgument("baselineText", baselineText);

        //Build the source.
        XYChartDataSource source = null;
        final String templateName = scalarOutputPlotTypeInfoMap.get(usedPlotType).getDefaultTemplateName();

        //Lead-threshold is the default.  This is for plots with the lead time on the domain axis and threshold in the legend.
        if(usedPlotType.equals(PlotTypeSelection.LEAD_THRESHOLD))
        {
            source = new ScalarOutputByLeadThresholdXYChartDataSource(0, input);

            //Legend title.
            final String legendTitle = "Thresholds";
            String legendUnitsText = "";
            if(input.hasQuantileThresholds())
            {
                legendUnitsText += " [" + meta.getInputDimension() + "]";
            }
            else if(input.keySetByThreshold().size() > 1)
            {
                legendUnitsText += " [" + meta.getInputDimension() + "]";
            }
            arguments.addArgument("legendTitle", legendTitle);
            arguments.addArgument("legendUnitsText", legendUnitsText);
        }
        //This is for plots with the threshold on the domain axis and lead time in the legend.
        else if(usedPlotType.equals(PlotTypeSelection.THRESHOLD_LEAD))
        {
            source = new ScalarOutputByThresholdLeadXYChartDataSource(0, input);

            //Legend title.
            arguments.addArgument("legendTitle", "Lead Times");
            arguments.addArgument("legendUnitsText", " [hours]");
        }
        else
        {
            throw new IllegalArgumentException("Plot type of " + userSpecifiedPlotType
                + " is not valid for a generic scalar output plot by lead/threshold.");
        }

        //Process override parameters.
        ChartDrawingParameters override = null;
        if(overrideParametersStr != null)//TRIM ONLY IF NOT NULL!
        {
            final String usedStr = overrideParametersStr.trim();
            if(!usedStr.isEmpty())
            {
                override = new ChartDrawingParameters();
                XMLTools.readXMLFromString(usedStr, override);
            }
        }

        //Build the ChartEngine instance.
        final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(source),
                                                               arguments,
                                                               templateName,
                                                               override);

        return engine;
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
     * @throws GenericXMLReadingHandlerException If the override XML cannot be parsed.
     */
    public static ChartEngine buildSingleValuedPairsChartEngine(final SingleValuedPairs input,
                                                                final String userSpecifiedTemplateResourceName,
                                                                final String overrideParametersStr) throws ChartEngineException,
                                                                                                    GenericXMLReadingHandlerException
    {

        String templateName = "singleValuedPairsTemplate.xml";
        if(userSpecifiedTemplateResourceName != null)
        {
            templateName = userSpecifiedTemplateResourceName;
        }

        //Build the source.
        final SingleValuedPairsXYChartDataSource source = new SingleValuedPairsXYChartDataSource(0, input);

        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor();

        //The following helper factory is part of the wres-datamodel, not the api. It will need to be supplied by 
        //(i.e. dependency injected from) wres-core as a MetadataFactory, which is part of the API
        final Metadata meta = input.getMetadata();
        final DatasetIdentifier identifier = meta.getIdentifier();

        //Setup fixed arguments.  This uses a special set since it is not metric output.
        arguments.addArgument("locationName", identifier.getGeospatialID());
        arguments.addArgument("variableName", identifier.getVariableID());
        arguments.addArgument("rangeAxisLabelPrefix", "Forecast");
        arguments.addArgument("domainAxisLabelPrefix", "Observed");
        arguments.addArgument("primaryScenario", identifier.getScenarioID());
        arguments.addArgument("inputUnitsText", " [" + meta.getDimension() + "]");

        //Process override parameters.
        ChartDrawingParameters override = null;
        if(overrideParametersStr != null)//TRIM ONLY IF NOT NULL!
        {
            final String usedStr = overrideParametersStr.trim();
            if(!usedStr.isEmpty())
            {
                override = new ChartDrawingParameters();
                XMLTools.readXMLFromString(usedStr, override);
            }
        }

        //Build the ChartEngine instance.
        final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(source),
                                                               arguments,
                                                               templateName,
                                                               override);

        return engine;
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
    private static XYChartDataSource constructConnectedPointsDataSource(final int sourceIndex, final Point2D... points)
    {
        final double[] xValues = new double[points.length];
        final double[] yValues = new double[points.length];
        for(int i = 0; i < points.length; i++)
        {
            xValues[i] = points[i].getX();
            yValues[i] = points[i].getY();
        }
        try
        {
            return new NumericalXYChartDataSource(null,
                                                  sourceIndex,
                                                  Lists.newArrayList(xValues),
                                                  Lists.newArrayList(yValues));
        }
        catch(final XYChartDataSourceException e)
        {
            throw new IllegalStateException("The DataSetXYChartDataSource does not throw exceptions, so how did I get here?");
        }
    }

    /**
     * Setups up default arguments that are for metric output plots.
     */
    private static WRESArgumentProcessor buildDefaultMetricOutputPlotArgumentsProcessor(final DataFactory factory,
                                                                                        final MetricOutputMetadata meta)
    {
        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor();
        final DatasetIdentifier identifier = meta.getIdentifier();

        //Setup fixed arguments.
        arguments.addArgument("locationName", identifier.getGeospatialID());
        arguments.addArgument("variableName", identifier.getVariableID());
        arguments.addArgument("primaryScenario", identifier.getScenarioID());
        arguments.addArgument("metricName", factory.getMetadataFactory().getMetricName(meta.getMetricID()));
        arguments.addArgument("metricShortName", factory.getMetadataFactory().getMetricShortName(meta.getMetricID()));
        arguments.addArgument("outputUnitsText", " [" + meta.getDimension() + "]");
        arguments.addArgument("inputUnitsText", " [" + meta.getInputDimension() + "]");

        //I could create a helper method to handle this wrapping, but I don't think this will be used outside of this context,
        //so why bother?  (This relates to an email James wrote.)
        if(meta.getMetricComponentID().equals(MetricConstants.MAIN))
        {
            arguments.addArgument("metricComponentNameSuffix", "");
        }
        else
        {
            arguments.addArgument("metricComponentNameSuffix",
                                  " " + factory.getMetadataFactory()
                                               .getMetricComponentName(meta.getMetricComponentID()));
        }

        return arguments;
    }

    /**
     * Stores information about each {@link VisualizationPlotType} necessary to validation user parameters, including
     * the default template name.
     * 
     * @author hank.herr
     */
    public static class PlotTypeInformation
    {
        private final Class<?> expectedPlotDataClass;
        private final Class<?> dataGenericType;
        private final String defaultTemplateName;

        public PlotTypeInformation(final Class<?> expectedPlotDataClass,
                                   final Class<?> dataGenericType,
                                   final String defaultTemplateName)
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

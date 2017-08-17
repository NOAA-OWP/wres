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

import com.google.common.collect.Lists;

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
     * Maintains information about the different plot types, including defaults and expected classes. Its unclear to me
     * if all of the fields stored in the {@link PlotTypeInformation} is needed, but I know the template name is.
     */
    private static HashMap<VisualizationPlotType, PlotTypeInformation> plotTypeInfoMap = new HashMap<>();
    static
    {
        plotTypeInfoMap.put(VisualizationPlotType.LEAD_THRESHOLD,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    ScalarOutput.class,
                                                    "scalarOutputLeadThresholdTemplate.xml"));
        plotTypeInfoMap.put(VisualizationPlotType.THRESHOLD_LEAD,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    ScalarOutput.class,
                                                    "scalarOutputThresholdLeadTemplate.xml"));
        plotTypeInfoMap.put(VisualizationPlotType.SINGLE_VALUED_PAIRS,
                            new PlotTypeInformation(SingleValuedPairs.class, null, "singleValuedPairsTemplate.xml"));
        plotTypeInfoMap.put(VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_LEAD,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    MultiVectorOutput.class,
                                                    "reliabilityDiagramTemplate.xml"));
        plotTypeInfoMap.put(VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_THRESHOLD,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    MultiVectorOutput.class,
                                                    "reliabilityDiagramTemplate.xml"));
        plotTypeInfoMap.put(VisualizationPlotType.ROC_DIAGRAM_FOR_LEAD,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    MultiVectorOutput.class,
                                                    "rocDiagramTemplate.xml"));
        plotTypeInfoMap.put(VisualizationPlotType.ROC_DIAGRAM_FOR_THRESHOLD,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    MultiVectorOutput.class,
                                                    "rocDiagramTemplate.xml"));
//        plotTypeInfoMap.put(VisualizationPlotType.QQ_DIAGRAM_FOR_LEAD,
//                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
//                                                    MultiVectorOutput.class,
//                                                    "reliabilityDiagramTemplate.xml"));
//        plotTypeInfoMap.put(VisualizationPlotType.QQ_DIAGRAM_FOR_THRESHOLD,
//                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
//                                                    MultiVectorOutput.class,
//                                                    "reliabilityDiagramTemplate.xml"));
    }

    /**
     * Defines the valid visualization plot types.
     */
    public static enum VisualizationPlotType
    {
        LEAD_THRESHOLD, THRESHOLD_LEAD, SINGLE_VALUED_PAIRS, RELIABILITY_DIAGRAM_FOR_LEAD, RELIABILITY_DIAGRAM_FOR_THRESHOLD, ROC_DIAGRAM_FOR_LEAD, ROC_DIAGRAM_FOR_THRESHOLD, QQ_DIAGRAM_FOR_LEAD, QQ_DIAGRAM_FOR_THRESHOLD
    };

    /**
     * @return The {@link VisualizationPlotType} that corresponds to the provided {@link PlotTypeSelection}.
     */
    public static VisualizationPlotType fromMetricConfigName(final PlotTypeSelection plotType) throws ChartEngineException
    {
        if(Objects.isNull(plotType))
        {
            return null;
        }
        switch(plotType)
        {
            case LEAD_THRESHOLD:
                return VisualizationPlotType.LEAD_THRESHOLD;
            case THRESHOLD_LEAD:
                return VisualizationPlotType.THRESHOLD_LEAD;
            case SINGLE_VALUED_PAIRS:
                return VisualizationPlotType.SINGLE_VALUED_PAIRS;
            default:
                throw new ChartEngineException("Unable to map the input plot type '" + plotType + "'.");
        }
    }

    /**
     * @param input The metric output to plot.
     * @param factory The data factory from which arguments will be identified.
     * @param userSpecifiedPlotType An optional plot type to generate, where multiple plot types are supported for the
     *            input. May be null.
     * @param userSpecifiedTemplateResourceName Name of the resource to load which provides the default template for
     *            chart construction. May be null to use default template identified in {@link #plotTypeInfoMap}.
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
                                                                                       final VisualizationPlotType userSpecifiedPlotType,
                                                                                       final String userSpecifiedTemplateResourceName,
                                                                                       final String overrideParametersStr) throws ChartEngineException,
                                                                                                                           GenericXMLReadingHandlerException
    {
        String templateName = null;
        final ConcurrentMap<Object, ChartEngine> results = new ConcurrentSkipListMap<>();

        //I'm  not a big fant of this, but...
        //This needs to be able to determine which key to loop over before actually diving into the loop.  And I want only one loop.  
        //So, I'm goint to have to do an if-check up front to identify the key set, and then an if-check within the loop to identify the plots.
        //Here is the first if-check...
        Set<?> keySetValues = input.keySetByLead();
        if(input.getMetadata().getMetricID() == MetricConstants.RELIABILITY_DIAGRAM)
        {
            if((userSpecifiedPlotType != null)
                && (userSpecifiedPlotType.equals(VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_THRESHOLD)))
            {
                keySetValues = input.keySetByThreshold();
            }
        }
        else if(input.getMetadata().getMetricID() == MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM)
        {
            if((userSpecifiedPlotType != null)
                && (userSpecifiedPlotType.equals(VisualizationPlotType.ROC_DIAGRAM_FOR_THRESHOLD)))
            {
                keySetValues = input.keySetByThreshold();
            }
        }
        else
        {
            throw new IllegalArgumentException("Unrecognized plot type of " + input.getMetadata().getMetricID()
                + " specified in the metric information.");
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
                if((userSpecifiedPlotType == null)
                    || (userSpecifiedPlotType.equals(VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_LEAD)))
                {
                    templateName = determineTemplateResourceName(userSpecifiedTemplateResourceName,
                                                                 userSpecifiedPlotType,
                                                                 VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_LEAD);
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
                                          "at Lead Hour " + inputSlice.getKey(0).getFirstKey().toString());
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
                else if(userSpecifiedPlotType.equals(VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_THRESHOLD))
                {
                    templateName =
                                 determineTemplateResourceName(userSpecifiedTemplateResourceName,
                                                               userSpecifiedPlotType,
                                                               VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_THRESHOLD);
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
                                          "for Threshold " + inputSlice.getKey(0).getSecondKey().toString() + " ("
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
                if((userSpecifiedPlotType == null)
                    || userSpecifiedPlotType.equals(VisualizationPlotType.ROC_DIAGRAM_FOR_LEAD))
                {
                    templateName = determineTemplateResourceName(userSpecifiedTemplateResourceName,
                                                                 userSpecifiedPlotType,
                                                                 VisualizationPlotType.ROC_DIAGRAM_FOR_LEAD);
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
                //Reliability diagram for each treshold, lead times in the legend.
                //-----------------------------------------------------------------
                else if(userSpecifiedPlotType.equals(VisualizationPlotType.ROC_DIAGRAM_FOR_THRESHOLD))
                {
//                    templateName =
//                                 determineTemplateResourceName(userSpecifiedTemplateResourceName,
//                                                               userSpecifiedPlotType,
//                                                               VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_THRESHOLD);
//                    final MetricOutputMapByLeadThreshold<MultiVectorOutput> inputSlice =
//                                                                                       input.sliceByThreshold((Threshold)keyInstance);
//
//                    //Setup the default arguments.
//                    final MetricOutputMetadata meta = inputSlice.getMetadata();
//                    arguments = buildDefaultMetricOutputPlotArgumentsProcessor(factory, meta);
//
//                    //Legend title and lead time argument are specific to the plot.
//                    arguments.addArgument("legendTitle", "Lead Time");
//                    arguments.addArgument("legendUnitsText", " [hours]");
//                    arguments.addArgument("leadHour", inputSlice.getKey(0).getFirstKey().toString());
//                    arguments.addArgument("diagramInstanceDescription",
//                                          "for Threshold " + inputSlice.getKey(0).getSecondKey().toString() + " ("
//                                              + meta.getInputDimension() + ")");
//                    arguments.addArgument("plotTitleVariable", "Thresholds");
//
//                    dataSources.add(new ReliabilityDiagramXYChartDataSource(0, inputSlice));
//                    dataSources.add(new ReliabilityDiagramSampleSizeXYChartDataSource(1, inputSlice));
//                    dataSources.add(constructConnectedPointsDataSource(2,
//                                                                       new Point2D.Double(0.0, 0.0),
//                                                                       new Point2D.Double(1.0, 1.0)));
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
     *            chart construction. May be null to use default template identified in {@link #plotTypeInfoMap}.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return
     * @throws ChartEngineException
     * @throws GenericXMLReadingHandlerException
     */
    public static ConcurrentMap<Object, ChartEngine> buildVectorOutputChartEngine(final MetricOutputMapByLeadThreshold<VectorOutput> input,
                                                                                  final DataFactory factory,
                                                                                  final VisualizationPlotType userSpecifiedPlotType,
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
     *            chart construction. May be null to use default template identified in {@link #plotTypeInfoMap}.
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
                                                                  final VisualizationPlotType userSpecifiedPlotType,
                                                                  final String userSpecifiedTemplateResourceName,
                                                                  final String overrideParametersStr) throws ChartEngineException,
                                                                                                      GenericXMLReadingHandlerException
    {
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
        String templateName = null;

        //Lead-threshold is the default.  This is for plots with the lead time on the domain axis and threshold in the legend.
        if(userSpecifiedPlotType == null || userSpecifiedPlotType.equals(VisualizationPlotType.LEAD_THRESHOLD))
        {
            templateName = determineTemplateResourceName(userSpecifiedTemplateResourceName,
                                                         userSpecifiedPlotType,
                                                         VisualizationPlotType.LEAD_THRESHOLD);
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
        else if(userSpecifiedPlotType.equals(VisualizationPlotType.THRESHOLD_LEAD))
        {
            templateName = determineTemplateResourceName(userSpecifiedTemplateResourceName,
                                                         userSpecifiedPlotType,
                                                         VisualizationPlotType.LEAD_THRESHOLD);
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

        final String templateName = determineTemplateResourceName(userSpecifiedTemplateResourceName,
                                                                  null,
                                                                  VisualizationPlotType.SINGLE_VALUED_PAIRS);

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
     * @param userSpecifiedName The user specified template name, which is always used if not null.
     * @param userSpecifiedPlotType The user specified plot type, which is used to access {@link #plotTypeInfoMap} to
     *            acquire the default template resource name, but only if it is not null.
     * @param defaultPlotType The fall back if neither of the other two are specified. If an entry in the
     *            {@link #plotTypeInfoMap} cannot be found, then an {@link IllegalStateException} will be thrown, since
     *            this reflects a run-time, coding error.
     * @return The template resource name, which may be either the name of something on the class path or a file name on
     *         the file system.
     */
    private static String determineTemplateResourceName(final String userSpecifiedName,
                                                        final VisualizationPlotType userSpecifiedPlotType,
                                                        final VisualizationPlotType defaultPlotType)
    {
        if(userSpecifiedName != null)
        {
            return userSpecifiedName;
        }
        if(userSpecifiedPlotType != null)
        {
            final PlotTypeInformation info = plotTypeInfoMap.get(userSpecifiedPlotType);
            if(info != null)
            {
                return info.getDefaultTemplateName();
            }
        }
        final PlotTypeInformation info = plotTypeInfoMap.get(defaultPlotType);
        if(info == null)
        {
            throw new IllegalStateException("The default plot type is being used to acquire plot type information, but it is not defined in the plotTypeInfoMap.");
        }
        return info.getDefaultTemplateName();
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

package wres.vis;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
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
import wres.datamodel.metric.DatasetIdentifier;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;

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
                                                    "scalarOutputTemplate.xml"));
        plotTypeInfoMap.put(VisualizationPlotType.THRESHOLD_LEAD,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    ScalarOutput.class,
                                                    "scalarOutputTemplate.xml"));
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
    }

    /**
     * Defines the valid visualization plot types.
     */
    public static enum VisualizationPlotType
    {
        LEAD_THRESHOLD, THRESHOLD_LEAD, SINGLE_VALUED_PAIRS, RELIABILITY_DIAGRAM_FOR_LEAD, RELIABILITY_DIAGRAM_FOR_THRESHOLD
    };

    /**
     * @param input The metric output to plot.
     * @param factory The metadata from which arguments will be identified.
     * @param plotType An optional plot type to generate, where multiple plot types are supported for the input. May be
     *            null.
     * @param templateResourceName Name of the resource to load which provides the default template for chart
     *            construction.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     * @throws GenericXMLReadingHandlerException If the override XML cannot be parsed.
     */
    public static ConcurrentMap<Integer, ChartEngine> buildMultiVectorOutputChartEngineByLead(final MetricOutputMapByLeadThreshold<MultiVectorOutput> input,
                                                                                              final MetadataFactory factory,
                                                                                              final VisualizationPlotType plotType,
                                                                                              final String templateResourceName,
                                                                                              final String overrideParametersStr) throws ChartEngineException,
                                                                                                                                  GenericXMLReadingHandlerException
    {
        final ConcurrentMap<Integer, ChartEngine> results = new ConcurrentSkipListMap<>();

        for(final Integer lead: input.keySetByLead())
        {
            final List<XYChartDataSource> dataSources = new ArrayList<>();
            WRESArgumentProcessor arguments = null;

            //For a reliability diagram, as specified by the metric id in the meta data...
            if(input.getMetadata().getMetricID() == MetricConstants.RELIABILITY_DIAGRAM)
            {
                //Plots for thresholds are not yet generated.
                if(plotType.equals(VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_THRESHOLD))
                {
                    throw new IllegalArgumentException("Reliability diagrms for a threshold (across all lead times) are not yet support.");
                }
                //Check for a bad plot type.
                else if(!plotType.equals(VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_LEAD))
                {
                    throw new IllegalArgumentException("Plot type " + plotType
                        + " is invalid for a reliability diagram.");
                }
                //Default reliability diagram will be VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_LEAD.
                else
                {
                    final MetricOutputMapByLeadThreshold<MultiVectorOutput> inputSlice = input.sliceByLead(lead);

                    //Setup the default arguments.
                    final MetricOutputMetadata meta = inputSlice.getMetadata();
                    arguments = buildDefaultMetricOutputPlotArgumentsProcessor(factory, meta);

                    //Legend title and lead time argument are specific to the plot.
                    //XXX Note that the code below is very specific to the input attribute AND whether this is against threshold or lead time.
                    //In other words, this chuck of code is completely specific to this method and cannot be used in the scalar output method
                    //which need to use other code.  At this point, there is no need to pull it out and place it in another method because 
                    //there are no other metrics in this method that need it.
                    final String legendTitle = "Thresholds";
                    String legendUnitsText = "";
                    if(input.hasQuantileThresholds())
                    {
                        legendUnitsText += " (" + meta.getInputDimension() + " [probability])";
                    }
                    else if(input.keySetByThreshold().size() > 1)
                    {
                        legendUnitsText += " (" + meta.getInputDimension() + ")";
                    }
                    arguments.addArgument("legendTitle", legendTitle);
                    arguments.addArgument("legendUnitsText", legendUnitsText);
                    arguments.addArgument("leadHour", inputSlice.getKey(0).getFirstKey().toString());

                    dataSources.add(new ReliabilityDiagramXYChartDataSource(0, inputSlice));
                    dataSources.add(new ReliabilityDiagramSampleSizeXYChartDataSource(1, inputSlice));
                    dataSources.add(constructConnectedPointsDataSource(2,
                                                                       new Point2D.Double(0.0, 0.0),
                                                                       new Point2D.Double(1.0, 1.0)));
                }
            }
            else
            {
                throw new IllegalArgumentException("Plot type of " + plotType
                    + " is not valid for a reliability diagram.");
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
            final ChartEngine engine = ChartTools.buildChartEngine(dataSources,
                                                                   arguments,
                                                                   templateResourceName,
                                                                   override);
            results.put(lead, engine);
        }
        return results;
    }

    /**
     * @param input The metric output to plot.
     * @param factory The metadata from which arguments will be identified.
     * @param plotType The plot type to generate. For this chart, the plot type must be either
     *            {@link VisualizationPlotType#LEAD_THRESHOLD} or {@link VisualizationPlotType#THRESHOLD_LEAD}. May be
     *            null and, if so, defaults to {@link VisualizationPlotType#LEAD_THRESHOLD}.
     * @param templateResourceName Name of the resource to load which provides the default template for chart
     *            construction.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     * @throws GenericXMLReadingHandlerException If the override XML cannot be parsed.
     */
    public static ChartEngine buildGenericScalarOutputChartEngine(final MetricOutputMapByLeadThreshold<ScalarOutput> input,
                                                                  final MetadataFactory factory,
                                                                  final VisualizationPlotType plotType,
                                                                  final String templateResourceName,
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

        //Lead-threshold is the default.
        if(plotType == null || plotType.equals(VisualizationPlotType.LEAD_THRESHOLD))
        {
            source = new ScalarOutputByLeadThresholdXYChartDataSource(0, input);

            //Legend title.
            final String legendTitle = "Thresholds";
            String legendUnitsText = "";
            if(input.hasQuantileThresholds())
            {
                legendUnitsText += " (" + meta.getInputDimension() + " [probability])";
            }
            else if(input.keySetByThreshold().size() > 1)
            {
                legendUnitsText += " (" + meta.getInputDimension() + ")";
            }
            arguments.addArgument("legendTitle", legendTitle);
            arguments.addArgument("legendUnitsText", legendUnitsText);
        }
        else if(plotType.equals(VisualizationPlotType.THRESHOLD_LEAD))
        {
            source = new ScalarOutputByThresholdLeadXYChartDataSource(0, input);

            //Legend title.
            arguments.addArgument("legendTitle", "Lead Time");
            arguments.addArgument("legendUnitsText", "");
        }
        else
        {
            throw new IllegalArgumentException("Plot type of " + plotType
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
                                                               templateResourceName,
                                                               override);

        return engine;
    }

    /**
     * @param input The pairs to plot.
     * @param templateResourceName Name of the resource to load which provides the default template for chart
     *            construction.
     * @param overrideParametersStr String of XML (top level tag: chartDrawingParameters) that specifies the user
     *            overrides for the appearance of chart.
     * @return A {@link ChartEngine} that can be used to build the {@link JFreeChart} and output the image. This can be
     *         passed to {@link ChartTools#generateOutputImageFile(java.io.File, JFreeChart, int, int)} in order to
     *         construct the image file.
     * @throws ChartEngineException If the {@link ChartEngine} fails to construct.
     * @throws GenericXMLReadingHandlerException If the override XML cannot be parsed.
     */
    public static ChartEngine buildSingleValuedPairsChartEngine(final SingleValuedPairs input,
                                                                final String templateResourceName,
                                                                final String overrideParametersStr) throws ChartEngineException,
                                                                                                    GenericXMLReadingHandlerException
    {
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
                                                               templateResourceName,
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
    private static WRESArgumentProcessor buildDefaultMetricOutputPlotArgumentsProcessor(final MetadataFactory factory,
                                                                                        final MetricOutputMetadata meta)
    {
        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor();
        final DatasetIdentifier identifier = meta.getIdentifier();

        //Setup fixed arguments.
        arguments.addArgument("locationName", identifier.getGeospatialID());
        arguments.addArgument("variableName", identifier.getVariableID());
        arguments.addArgument("primaryScenario", identifier.getScenarioID());
        arguments.addArgument("metricName", factory.getMetricName(meta.getMetricID()));
        arguments.addArgument("metricShortName", factory.getMetricShortName(meta.getMetricID()));
        arguments.addArgument("outputUnitsText", " [" + meta.getDimension() + "]");
        arguments.addArgument("inputUnitsText", " [" + meta.getInputDimension() + "]");

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

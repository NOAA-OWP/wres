package wres.vis;

import java.util.HashMap;
import java.util.Objects;

import org.jfree.chart.JFreeChart;

import com.google.common.collect.Lists;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.parameters.ChartDrawingParameters;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import ohd.hseb.hefs.utils.xml.XMLTools;
import wres.datamodel.metric.DatasetIdentifier;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
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
     * Maintains information about the different plot types, including defaults and expected classes.
     */
    //TODO I have put this in place in case it comes in handy, later.  However, its uncelar to me that
    //this will be needed.  Until such time (i.e., when Jesse asks for it), I'm going to halt development
    //of this map.
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
        plotTypeInfoMap.put(VisualizationPlotType.RELIABILITY_DIAGRAM,
                            new PlotTypeInformation(MetricOutputMapByLeadThreshold.class,
                                                    MultiVectorOutput.class,
                                                    "reliabiityDiagramTemplate.xml"));
    }

    /**
     * Defines the valid visualization plot types.
     */
    public static enum VisualizationPlotType
    {
        LEAD_THRESHOLD, THRESHOLD_LEAD, SINGLE_VALUED_PAIRS, RELIABILITY_DIAGRAM
    };

    /**
     * @param input The metric output to plot.
     * @param factory The metadata from which arguments will be identified.
     * @param plotType The plot type to generate. For this chart, the plot type must be
     *            {@link VisualizationPlotType#RELIABILITY_DIAGRAM}.
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
    public static ChartEngine buildMultiVectorOutputChartEngine(final MetricOutputMapByLeadThreshold<MultiVectorOutput> input,
                                                                final MetadataFactory factory,
                                                                final VisualizationPlotType plotType,
                                                                final String templateResourceName,
                                                                final String overrideParametersStr) throws ChartEngineException,
                                                                                                    GenericXMLReadingHandlerException
    {
        //Build the sources.
        XYChartDataSource reliabilitySource = null;
        XYChartDataSource sampleSizeSource = null;
        if(plotType.equals(VisualizationPlotType.RELIABILITY_DIAGRAM))
        {
            reliabilitySource = new ReliabilityDiagramXYChartDataSource(0, input);
            sampleSizeSource = new ReliabilityDiagramSampleSizeXYChartDataSource(1, input);
        }
        else
        {
            throw new IllegalArgumentException("Plot type of " + plotType + " is not valid for a reliability diagram.");
        }

        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor();

        final MetricOutputMetadata meta = input.getMetadata();
        final DatasetIdentifier identifier = meta.getIdentifier();

        //Setup fixed arguments.
        arguments.addArgument("locationName", identifier.getGeospatialID());
        arguments.addArgument("variableName", identifier.getVariableID());
        arguments.addArgument("primaryScenario", identifier.getScenarioID());
        arguments.addArgument("metricName", factory.getMetricName(meta.getMetricID()));
        arguments.addArgument("metricShortName", factory.getMetricShortName(meta.getMetricID()));
        arguments.addArgument("outputUnitsText", " [" + meta.getDimension() + "]");
        arguments.addArgument("inputUnitsText", " [" + meta.getInputDimension() + "]");

        //Specific to this plot.
        arguments.addArgument("leadHour", input.getKey(0).getFirstKey().toString());

        //Setup conditional arguments
        String baselineText = "";
        if(!Objects.isNull(identifier.getScenarioIDForBaseline()))
        {
            baselineText = " against predictions from " + identifier.getScenarioIDForBaseline();
        }
        arguments.addArgument("baselineText", baselineText);

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
        final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(reliabilitySource, sampleSizeSource),
                                                               arguments,
                                                               templateResourceName,
                                                               override);

        return engine;
    }

    /**
     * @param input The metric output to plot.
     * @param factory The metadata from which arguments will be identified.
     * @param plotType The plot type to generate. For this chart, the plot type must be either
     *            {@link VisualizationPlotType#LEAD_THRESHOLD} or {@link VisualizationPlotType#THRESHOLD_LEAD}.
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
        //Build the source.
        XYChartDataSource source = null;
        if(plotType.equals(VisualizationPlotType.LEAD_THRESHOLD))
        {
            source = new ScalarOutputByLeadThresholdXYChartDataSource(0, input);
        }
        else if(plotType.equals(VisualizationPlotType.THRESHOLD_LEAD))
        {
            source = new ScalarOutputByThresholdLeadXYChartDataSource(0, input);
        }
        else
        {
            throw new IllegalArgumentException("Plot type of " + plotType
                + " is not valid for a generic scalar output plot by lead/threshold.");
        }

        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor();

        final MetricOutputMetadata meta = input.getMetadata();
        final DatasetIdentifier identifier = meta.getIdentifier();

        //Setup fixed arguments.
        arguments.addArgument("locationName", identifier.getGeospatialID());
        arguments.addArgument("variableName", identifier.getVariableID());
        arguments.addArgument("primaryScenario", identifier.getScenarioID());
        arguments.addArgument("metricName", factory.getMetricName(meta.getMetricID()));
        arguments.addArgument("metricShortName", factory.getMetricShortName(meta.getMetricID()));
        arguments.addArgument("outputUnitsText", " [" + meta.getDimension() + "]");
        arguments.addArgument("inputUnitsText", " [" + meta.getInputDimension() + "]");

        //Setup conditional arguments
        String baselineText = "";
        if(!Objects.isNull(identifier.getScenarioIDForBaseline()))
        {
            baselineText = " against predictions from " + identifier.getScenarioIDForBaseline();
        }
        arguments.addArgument("baselineText", baselineText);

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

        //Setup fixed arguments.
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
     * Stores information about each {@link VisualizationPlotType} necessary to validation user parameters.
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

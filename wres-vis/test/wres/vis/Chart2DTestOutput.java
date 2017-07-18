package wres.vis;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.junit.Assert;

import com.google.common.collect.Lists;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import junit.framework.TestCase;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.parameters.ChartDrawingParameters;
import ohd.hseb.hefs.utils.junit.FileComparisonUtilities;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import ohd.hseb.hefs.utils.xml.XMLTools;
import wres.datamodel.metric.DatasetIdentifier;
import wres.datamodel.metric.DefaultMetadataFactory;
import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.MapBiKey;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.Quantile;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.Threshold;
import wres.datamodel.metric.Threshold.Condition;

/**
 * Tests the construction of a 3D chart of metric outputs.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Chart2DTestOutput extends TestCase
{
    /**
     * Generate a plot by lead time on the domain axis.
     */

    public void test2ScalarOutput()
    {
        final String scenarioName = "test2";
        final File outputImageFile = new File("testoutput/chart2DTest/" + scenarioName + "_output.png");
        outputImageFile.delete();

        //Construct some single-valued pairs
        final MetricOutputMapByLeadThreshold<ScalarOutput> input = getMetricOutputMapByLeadThresholdOne();

        try
        {
            //Call the factory.
            final ChartEngine engine = buildGenericScalarOutputChartEngine(input,
                                                                           VisualizationPlotType.LEAD_THRESHOLD,
                                                                           "scalarOutputTemplate.xml",
                                                                           null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(outputImageFile, engine.buildChart(), 800, 600);

            //Compare against OS specific image benchmark.
            FileComparisonUtilities.assertImageFileSimilarToBenchmark(outputImageFile,
                                                                      new File("testinput/chart2DTest/benchmark."
                                                                          + scenarioName + "_output.png"),
                                                                      8,
                                                                      true,
                                                                      false);
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    /**
     * Generate a plot by threshold on the domain axis.
     */

    public void test3ScalarOutput()
    {
        final String scenarioName = "test3";
        final File outputImageFile = new File("testoutput/chart2DTest/" + scenarioName + "_output.png");
        outputImageFile.delete();

        //Construct some single-valued pairs
        final MetricOutputMapByLeadThreshold<ScalarOutput> input = getMetricOutputMapByLeadThresholdTwo();

        try
        {

            //Call the factory.
            final ChartEngine engine = buildGenericScalarOutputChartEngine(input,
                                                                           VisualizationPlotType.THRESHOLD_LEAD,
                                                                           "scalarOutputTemplate.xml",
                                                                           null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(outputImageFile, engine.buildChart(), 800, 600);

            //Compare against OS specific image benchmark.
            FileComparisonUtilities.assertImageFileSimilarToBenchmark(outputImageFile,
                                                                      new File("testinput/chart2DTest/benchmark."
                                                                          + scenarioName + "_output.png"),
                                                                      8,
                                                                      true,
                                                                      false);
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link ScalarOutput} comprising the CRPSS for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */

    private static MetricOutputMapByLeadThreshold<ScalarOutput> getMetricOutputMapByLeadThreshold()
    {
        final MetricOutputFactory outputFactory = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<MapBiKey<Integer, Threshold>, ScalarOutput> rawData = new TreeMap<>();

        try
        {
            //Create the input file
            final File resultFile = new File("testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not available in the test input file
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("CMS"),
                                                                            MetricConstants.MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                            MetricConstants.MAIN,
                                                                            metaFactory.getDatasetIdentifier("NPTP1",
                                                                                                             "STREAMFLOW",
                                                                                                             "HEFS",
                                                                                                             "ESP"));

            //Iterate through the lead times
            while(d.hasNext())
            {
                //Set the lead time
                final double leadTime = (Double)d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold)data.getResult(leadTime);
                final Iterator<MetricResultKey> e = t.getIterator();
                //Iterate through the thresholds
                while(e.hasNext())
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter)e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    //Read only selected quantiles to avoid polluting the plot
                    final Quantile q = outputFactory.getQuantile(constants[0], probConstants[0], Condition.GREATER);
                    final MapBiKey<Integer, Threshold> key = outputFactory.getMapKey((int)leadTime, q);

                    //Build the scalar result
                    final MetricResult result = t.getResult(f);
                    final double[] res = ((DoubleMatrix1DResult)result).getResult().toArray();
                    final ScalarOutput value = outputFactory.ofScalarOutput(res[0], meta);

                    //Append result
                    rawData.put(key, value);
                }
            }

        }
        catch(final Exception e)
        {
            e.printStackTrace();
            Assert.fail("Test failed : " + e.getMessage());
        }
        return outputFactory.ofMap(rawData);
    }

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link ScalarOutput} comprising the CRPSS for a subset of
     * thresholds and forecast lead times. Reads the input data from {@link #getMetricOutputMapByLeadThreshold()} and
     * slices.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByLeadThreshold<ScalarOutput> getMetricOutputMapByLeadThresholdOne()
    {
        final MetricOutputFactory outputFactory = DefaultMetricOutputFactory.getInstance();
        final MetricOutputMapByLeadThreshold<ScalarOutput> full = getMetricOutputMapByLeadThreshold();
        final List<MetricOutputMapByLeadThreshold<ScalarOutput>> combine = new ArrayList<>();
        final double[][] allow = new double[][]{{Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}, {0.5, 2707.5},
            {0.95, 13685.0}, {0.99, 26648.0}};
        for(final double[] next: allow)
        {
            combine.add(full.sliceByThreshold(outputFactory.getQuantile(next[1], next[0], Condition.GREATER)));
        }
        return outputFactory.combine(combine);
    }

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link ScalarOutput} comprising the CRPSS for a subset of
     * thresholds and forecast lead times. Reads the input data from {@link #getMetricOutputMapByLeadThreshold()} and
     * slices.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByLeadThreshold<ScalarOutput> getMetricOutputMapByLeadThresholdTwo()
    {
        final MetricOutputFactory outputFactory = DefaultMetricOutputFactory.getInstance();
        final MetricOutputMapByLeadThreshold<ScalarOutput> full = getMetricOutputMapByLeadThreshold();
        final List<MetricOutputMapByLeadThreshold<ScalarOutput>> combine = new ArrayList<>();
        final int[] allow = new int[]{42, 258, 474, 690};
        for(final int next: allow)
        {
            combine.add(full.sliceByLead(next));
        }
        return outputFactory.combine(combine);
    }

    /**
     * Presently valid variables includes only lead time and threshold.
     * 
     * @author Hank.Herr
     */
    public static enum VisualizationPlotType
    {
        LEAD_THRESHOLD, THRESHOLD_LEAD
    };

    /**
     * @param input The metrhic output is the input to this method.
     * @param domainVar The variable to display along the domain axis.
     * @param legendVar The variable defining the legend entries.
     * @param templateFile REMOVE THIS IN THE FUTURE! Eventually, for standard scalar output charts, a single template
     *            will be constructed under nonsrc and specified and used as the template file.
     * @param overrideParametersStr The configuration XML, as a {@link String}, specifying the user override of the
     *            chart drawing parameters.
     * @return A {@link ChartEngine} that can either be used to generate an image file or displayed in a GUI.
     * @throws ChartEngineException If the {@link ChartEngine} construction fails.
     * @throws GenericXMLReadingHandlerException If the provided override parameters cannot be parsed.
     */
    public static ChartEngine buildGenericScalarOutputChartEngine(final MetricOutputMapByLeadThreshold<ScalarOutput> input,
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
        else if (plotType.equals(VisualizationPlotType.THRESHOLD_LEAD))
        {
            source = new ScalarOutputByThresholdLeadXYChartDataSource(0, input);
        }

        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor();

        final MetricOutputMetadata meta = input.getMetadata();
        
        //The following helper factory is part of the wres-datamodel, not the api. It will need to be supplied by 
        //(i.e. dependency injected from) wres-core as a MetadataFactory, which is part of the API
        final MetadataFactory factory = DefaultMetadataFactory.getInstance();
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
        if(overrideParametersStr != null)
        {
            override = new ChartDrawingParameters();
            XMLTools.readXMLFromString(overrideParametersStr, override);
        }

        //Build the ChartEngine instance.
        final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(source),
                                                               arguments,
                                                               templateResourceName,
                                                               override);

        return engine;
    }
}

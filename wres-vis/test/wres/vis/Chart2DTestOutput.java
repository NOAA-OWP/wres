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
import ohd.hseb.charter.ChartTools;
import ohd.hseb.hefs.utils.junit.FileComparisonUtilities;
import wres.datamodel.metric.DatasetIdentifier;
import wres.datamodel.metric.DefaultMetadataFactory;
import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.Dimension;
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

    public void test1ScalarOutput()
    {
        //Construct some single-valued pairs
        final MetricOutputMapByLeadThreshold<ScalarOutput> input = getMetricOutputMapByLeadThresholdOne();

        //Construct the source from the pairs assigning it a data source order index of 0.  
        //The order index indicates the order in which the different sources are rendered.
        final ScalarOutputByLeadThresholdXYChartDataSource source =
                                                                  new ScalarOutputByLeadThresholdXYChartDataSource(0,
                                                                                                                   input);

        final String scenarioName = "test2";
        try
        {
            //The arguments processor for example purposes.
            final WRESArgumentProcessor arguments = new WRESArgumentProcessor();

            final MetricOutputMetadata meta = input.getMetadata();
            //The following helper factory is part of the wres-datamodel, not the api. It will need to be supplied by 
            //(i.e. dependency injected from) wres-core as a MetadataFactory, which is part of the API
            final MetadataFactory factory = DefaultMetadataFactory.getInstance();

            final DatasetIdentifier identifier = meta.getIdentifier();
            final String locationName = identifier.getGeospatialID();
            final String variableName = identifier.getVariableID();
            final String metricName = factory.getMetricName(meta.getMetricID());
            final String metricShortName = factory.getMetricShortName(meta.getMetricID());
            final String primaryScenario = identifier.getScenarioID();
            final String baselineScenario = identifier.getScenarioIDForBaseline(); //Not null if skill
            final Dimension units = meta.getDimension();

            //Compose a plot title
            String title = metricName + " for " + primaryScenario + " predictions of " + variableName + " at "
                + locationName;
            if(!Objects.isNull(baselineScenario))
            {
                title = title + " against predictions from " + baselineScenario;
            }
            //Set the range axis name and units
            final String rangeAxis = metricShortName + " [" + units + "]";

            //Set the arguments
            arguments.addArgument("plotTitle", title);
            arguments.addArgument("rangeAxis", rangeAxis);
            //No lead time units in metadata API: could add this easily, but probably a global WRES assumption
            arguments.addArgument("domainAxis", "FORECAST LEAD TIME [HOUR]");

            //Build the ChartEngine instance.
            final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(source),
                                                                   arguments,
                                                                   "testinput/chart2DTest/" + scenarioName
                                                                       + "_template.xml",
                                                                   null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + scenarioName + "_output.png"),
                                               engine.buildChart(),
                                               800,
                                               600);

            //Compare against OS specific image benchmark.
            FileComparisonUtilities.assertImageFileSimilarToBenchmark(new File("testoutput/chart2DTest/" + scenarioName
                + "_output.png"),
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

    public void test2ScalarOutput()
    {
        //Construct some single-valued pairs
        final MetricOutputMapByLeadThreshold<ScalarOutput> input = getMetricOutputMapByLeadThresholdTwo();

        //Construct the source from the pairs assigning it a data source order index of 0.  
        //The order index indicates the order in which the different sources are rendered.
        final ScalarOutputByThresholdLeadXYChartDataSource source =
                                                                  new ScalarOutputByThresholdLeadXYChartDataSource(0,
                                                                                                                   input);

        final String scenarioName = "test3";
        try
        {
            //The arguments processor for example purposes.
            final WRESArgumentProcessor arguments = new WRESArgumentProcessor();

            final MetricOutputMetadata meta = input.getMetadata();
            //The following helper factory is part of the wres-datamodel, not the api. It will need to be supplied by 
            //(i.e. dependency injected from) wres-core as a MetadataFactory, which is part of the API
            final MetadataFactory factory = DefaultMetadataFactory.getInstance();

            final DatasetIdentifier identifier = meta.getIdentifier();
            final String locationName = identifier.getGeospatialID();
            final String variableName = identifier.getVariableID();
            final String metricName = factory.getMetricName(meta.getMetricID());
            final String metricShortName = factory.getMetricShortName(meta.getMetricID());
            final String primaryScenario = identifier.getScenarioID();
            final String baselineScenario = identifier.getScenarioIDForBaseline(); //Not null if skill
            final Dimension outputUnits = meta.getDimension();
            final Dimension inputUnits = meta.getInputDimension();

            //Compose a plot title
            String title = metricName + " for " + primaryScenario + " predictions of " + variableName + " at "
                + locationName;
            if(!Objects.isNull(baselineScenario))
            {
                title = title + " against predictions from " + baselineScenario;
            }
            //Set the range axis name and units
            final String rangeAxis = metricShortName + " [" + outputUnits + "]";

            //Set the arguments
            arguments.addArgument("plotTitle", title);
            arguments.addArgument("rangeAxis", rangeAxis);
            arguments.addArgument("domainAxis", "THRESHOLD VALUE [" + inputUnits + "]");

            //Build the ChartEngine instance.
            final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(source),
                                                                   arguments,
                                                                   "testinput/chart2DTest/" + scenarioName
                                                                       + "_template.xml",
                                                                   null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + scenarioName + "_output.png"),
                                               engine.buildChart(),
                                               800,
                                               600);

            //Compare against OS specific image benchmark.
            FileComparisonUtilities.assertImageFileSimilarToBenchmark(new File("testoutput/chart2DTest/" + scenarioName
                + "_output.png"),
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

}

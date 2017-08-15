package wres.vis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.DoubleMatrix2DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import junit.framework.TestCase;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.hefs.utils.junit.FileComparisonUtilities;
import ohd.hseb.hefs.utils.tools.FileTools;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.DefaultMetadataFactory;
import wres.datamodel.metric.MapBiKey;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.QuantileThreshold;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.Threshold;
import wres.datamodel.metric.Threshold.Operator;

/**
 * Tests the construction of a 3D chart of metric outputs.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Chart2DTestOutput extends TestCase
{
    //TODO Note that test1 is within the Chart2DTestInput.java unit tests.  The two unit tests need to either be completely separate
    //(different testoutput, testinput directories) or merged.  
    //
    //do the following: Keep the unit tests separate; modify scenario names (inputTest1 and outputTest1) and update the file names.
    //That way the tests can be separate but generate output and using input from same directories.
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
            //Get an implementation of the metadata factory to use for testing.
            final MetadataFactory factory = DefaultMetadataFactory.getInstance();

            //Call the factory.
            final ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(input,
                                                                                              factory,
                                                                                              ChartEngineFactory.VisualizationPlotType.LEAD_THRESHOLD,
                                                                                              null,
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
            //Get an implementation of the metadata factory to use for testing.
            final MetadataFactory factory = DefaultMetadataFactory.getInstance();

            //Call the factory.
            final ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(input,
                                                                                              factory,
                                                                                              ChartEngineFactory.VisualizationPlotType.THRESHOLD_LEAD,
                                                                                              null,
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

    public void test4ReliabilityDiagram()
    {
        final String scenarioName = "test4";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles(new File("testoutput/chart2DTest/"), outputImageFileSuffix);
        }
        catch(final IOException e)
        {
            fail("Unexpected exception occurred trying to remove files: " + e.getMessage());
        }

        final MetricOutputMapByLeadThreshold<MultiVectorOutput> results = getReliabilityDiagramByLeadThreshold();

//DEBUG OUTPUT:
//        results42.forEach((key,result)-> {
//            System.out.println(key.getSecondKey());
//            System.out.println(Arrays.toString(result.get(MetricConstants.FORECAST_PROBABILITY).getDoubles()));  //This array forms the domain
//            System.out.println(Arrays.toString(result.get(MetricConstants.OBSERVED_GIVEN_FORECAST_PROBABILITY).getDoubles()));  //This array forms the range for reliability diagram subplot
//            System.out.println(Arrays.toString(result.get(MetricConstants.SAMPLE_SIZE).getDoubles())); //This array forms the range for the sample size subplot
//        });

        try
        {
            //Get an implementation of the metadata factory to use for testing.
            final MetadataFactory factory = DefaultMetadataFactory.getInstance();

            //Call the factory.
            final Map<Integer, ChartEngine> engineMap =
                                                      ChartEngineFactory.buildMultiVectorOutputChartEngineByLead(results,
                                                                                                                 factory,
                                                                                                                 ChartEngineFactory.VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_LEAD,
                                                                                                                 null,
                                                                                                                 null);

            //Generate the output file.
            for(final Integer lead: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + lead + "h."
                    + outputImageFileSuffix), engineMap.get(lead).buildChart(), 800, 600);

            }

            //Compare against OS specific image benchmark.
            for(final Integer lead: engineMap.keySet())
            {
                FileComparisonUtilities.assertImageFileSimilarToBenchmark(new File("testoutput/chart2DTest/" + lead
                    + "h." + outputImageFileSuffix),
                                                                          new File("testinput/chart2DTest/benchmark."
                                                                              + lead + "h." + outputImageFileSuffix),
                                                                          8,
                                                                          true,
                                                                          false);
            }
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
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
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByLeadThreshold<ScalarOutput> full = getMetricOutputMapByLeadThreshold();
        final List<MetricOutputMapByLeadThreshold<ScalarOutput>> combine = new ArrayList<>();
        final double[][] allow = new double[][]{{Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY}, {0.5, 2707.5},
            {0.95, 13685.0}, {0.99, 26648.0}};
        for(final double[] next: allow)
        {
            combine.add(full.sliceByThreshold(outputFactory.getQuantileThreshold(next[1], next[0], Operator.GREATER)));
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
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
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
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link ScalarOutput} comprising the CRPSS for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */

    private static MetricOutputMapByLeadThreshold<ScalarOutput> getMetricOutputMapByLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
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
                                                                            MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
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
                    final QuantileThreshold q = outputFactory.getQuantileThreshold(constants[0],
                                                                                   probConstants[0],
                                                                                   Operator.GREATER);
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
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link MultiVectorOutput} that contains the components of the
     * reliability diagram (forecast probabilities, observed given forecast probabilities, and sample sizes) for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getReliabilityDiagramByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */

    private static MetricOutputMapByLeadThreshold<MultiVectorOutput> getReliabilityDiagramByLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<MapBiKey<Integer, Threshold>, MultiVectorOutput> rawData = new TreeMap<>();
        //Read only selected quantiles
        final List<QuantileThreshold> allowed = new ArrayList<>();
        final double[][] allow = new double[][]{{0.1, 858.04}, {0.5, 2707.5}, {0.9, 9647.0}, {0.95, 13685.0}};
        for(final double[] next: allow)
        {
            allowed.add(outputFactory.getQuantileThreshold(next[1], next[0], Operator.GREATER));
        }
        try
        {
            //Create the input file
            final File resultFile = new File("testinput/chart2DTest/getReliabilityDiagramByLeadThreshold.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not available in the test input file
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("CMS"),
                                                                            MetricConstants.RELIABILITY_DIAGRAM,
                                                                            MetricConstants.MAIN,
                                                                            metaFactory.getDatasetIdentifier("NPTP1",
                                                                                                             "STREAMFLOW",
                                                                                                             "HEFS"));

            //Iterate through the lead times.
            int count = -1;
            while(d.hasNext())
            {
                //Hank: I'm going to start with the first and include every six: 0, 6, 12, etc.
                count++;
                if(count % 6 != 0)
                {
                    d.next();
                    continue;
                }

                //Set the lead time
                final double leadTime = (Double)d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold)data.getResult(leadTime);
                final Iterator<MetricResultKey> e = t.getIterator();
                boolean firstOne = true; //Used to track if this is the first time through the e loop.  See HDH comment below.
                
                //Iterate through the thresholds
                while(e.hasNext())
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter)e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final QuantileThreshold q = outputFactory.getQuantileThreshold(constants[0],
                                                                                   probConstants[0],
                                                                                   Operator.GREATER);
                    //Read only selected quantiles
                    if(allowed.contains(q))
                    {
                        final MapBiKey<Integer, Threshold> key = outputFactory.getMapKey((int)leadTime, q);

                        //Build the result
                        final MetricResult result = t.getResult(f);
                        final double[][] res = ((DoubleMatrix2DResult)result).getResult().toArray();
                        
                        //Ensure missings are NaN by brute force.
                        for (int i = 0; i < res.length; i ++)
                        {
                            for (int j = 0; j < res[i].length; j ++)
                            {
                                if (res[i][j] == -999D)
                                {
                                    res[i][j] = Double.NaN;
                                }
                                
                                //HDH (8/15/17): Forcing a NaN in the first time series within the reliability diagram at index 2.
                                if (firstOne && (i == 0) && (j == 2))
                                {
                                    res[i][j] = Double.NaN;
                                    firstOne = false;
                                }
                            }
                        }
                        
                        final Map<MetricConstants, double[]> output = new EnumMap<>(MetricConstants.class);
                        output.put(MetricConstants.FORECAST_PROBABILITY, res[0]); //Forecast probabilities
                        output.put(MetricConstants.OBSERVED_GIVEN_FORECAST_PROBABILITY, res[1]); //Observed | forecast probabilities
                        output.put(MetricConstants.SAMPLE_SIZE, res[2]); //Observed | forecast probabilities
                        final MultiVectorOutput value = outputFactory.ofMultiVectorOutput(output, meta);

                        //Append result
                        rawData.put(key, value);
                    }
                }
            }

        }
        catch(final Exception e)
        {
            e.printStackTrace();
            Assert.fail("Test failed : " + e.getMessage());
        }
        //Return the results
        return outputFactory.ofMap(rawData);
    }

}

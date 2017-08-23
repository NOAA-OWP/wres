package wres.vis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

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
import wres.config.generated.PlotTypeSelection;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MapBiKey;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMapByLeadThreshold;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.QuantileThreshold;
import wres.datamodel.ScalarOutput;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.VectorOutput;

/**
 * Tests the construction of a 3D chart of metric outputs. The building of the charts and images is tested via the unit
 * tests. To compare the images with benchmarks in testinput, YOU MUST EXECUTE THIS CLASS' MAIN!
 * 
 * @author hank.herr
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
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(input,
                                                                                              factory,
                                                                                              PlotTypeSelection.LEAD_THRESHOLD,
                                                                                              null,
                                                                                              null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(outputImageFile, engine.buildChart(), 800, 600);
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
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(input,
                                                                                              factory,
                                                                                              PlotTypeSelection.THRESHOLD_LEAD,
                                                                                              null,
                                                                                              null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(outputImageFile, engine.buildChart(), 800, 600);
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    /**
     * Generates multiple reliability diagrams, one for each lead time.
     */
    public void test4ReliabilityDiagramByLeadTime()
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
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine(results,
                                                                                                            factory,
                                                                                                            PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                            null,
                                                                                                            null);

            //Generate the output file.
            for(final Object lead: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + lead + "h."
                    + outputImageFileSuffix), engineMap.get(lead).buildChart(), 800, 600);

            }
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    /**
     * Generates multiple reliability diagrams, one for each threshold.
     */
    public void test5ReliabilityDiagramByThreshold()
    {
        final String scenarioName = "test5";
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

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine(results,
                                                                                                            factory,
                                                                                                            PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                            null,
                                                                                                            null);

            //Generate the output file.
            for(final Object thresh: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/"
                    + ((Threshold)thresh).getThreshold() + "." + outputImageFileSuffix),
                                                   engineMap.get(thresh).buildChart(),
                                                   800,
                                                   600);

            }
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    /**
     * Generates multiple plots, one for each vector index, by calling the scalar plots repeatedly.
     */
    public void test6VectorMetricOutput()
    {
        final String scenarioName = "test6";
        final String outputImageFileSuffix = scenarioName + "_output.png";
        try
        {
            FileTools.deleteFiles(new File("testoutput/chart2DTest/"), outputImageFileSuffix);
        }
        catch(final IOException e)
        {
            fail("Unexpected exception occurred trying to remove files: " + e.getMessage());
        }

        //Construct some single-valued pairs
        final MetricOutputMapByLeadThreshold<VectorOutput> input = getVectorMetricOutputMapByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final ConcurrentMap<Object, ChartEngine> engineMap = ChartEngineFactory.buildVectorOutputChartEngine(input,
                                                                                                                 factory,
                                                                                                                 PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                                 null,
                                                                                                                 null);
            //Generate the output file.
            for(final Object key: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + key + "."
                    + outputImageFileSuffix), engineMap.get(key).buildChart(), 800, 600);

            }
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    public void test7ROCDiagramByLeadTime()
    {
        final String scenarioName = "test7";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles(new File("testoutput/chart2DTest/"), outputImageFileSuffix);
        }
        catch(final IOException e)
        {
            fail("Unexpected exception occurred trying to remove files: " + e.getMessage());
        }

        final MetricOutputMapByLeadThreshold<MultiVectorOutput> results = getROCDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine(results,
                                                                                                            factory,
                                                                                                            PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                            null,
                                                                                                            null);

            //Generate the output file.
            for(final Object lead: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + lead + "h."
                    + outputImageFileSuffix), engineMap.get(lead).buildChart(), 800, 600);

            }
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    public void test8ROCDiagramByThreshold()
    {
        final String scenarioName = "test8";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles(new File("testoutput/chart2DTest/"), outputImageFileSuffix);
        }
        catch(final IOException e)
        {
            fail("Unexpected exception occurred trying to remove files: " + e.getMessage());
        }

        final MetricOutputMapByLeadThreshold<MultiVectorOutput> results = getROCDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine(results,
                                                                                                            factory,
                                                                                                            PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                            null,
                                                                                                            null);

            //Generate the output file.
            for(final Object thresh: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/"
                    + ((Threshold)thresh).getThreshold() + "." + outputImageFileSuffix),
                                                   engineMap.get(thresh).buildChart(),
                                                   800,
                                                   600);

            }
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    public void test9QQDiagramByLeadTime()
    {
        final String scenarioName = "test9";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles(new File("testoutput/chart2DTest/"), outputImageFileSuffix);
        }
        catch(final IOException e)
        {
            fail("Unexpected exception occurred trying to remove files: " + e.getMessage());
        }

        final MetricOutputMapByLeadThreshold<MultiVectorOutput> results = getQQDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine(results,
                                                                                                            factory,
                                                                                                            PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                            null,
                                                                                                            null);

            //Generate the output file.
            for(final Object lead: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + lead + "h."
                    + outputImageFileSuffix), engineMap.get(lead).buildChart(), 800, 600);

            }
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    public void test10QQDiagramByThreshold()
    {
        final String scenarioName = "test8";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles(new File("testoutput/chart2DTest/"), outputImageFileSuffix);
        }
        catch(final IOException e)
        {
            fail("Unexpected exception occurred trying to remove files: " + e.getMessage());
        }

        final MetricOutputMapByLeadThreshold<MultiVectorOutput> results = getQQDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine(results,
                                                                                                            factory,
                                                                                                            PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                            null,
                                                                                                            null);

            //Generate the output file.
            for(final Object thresh: engineMap.keySet())
            {
                ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/" + "alldata" + "."
                    + outputImageFileSuffix), engineMap.get(thresh).buildChart(), 800, 600);

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
     * thresholds and forecast lead times. Reads the input data from {@link #getScalarMetricOutputMapByLeadThreshold()}
     * and slices.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByLeadThreshold<ScalarOutput> getMetricOutputMapByLeadThresholdOne()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByLeadThreshold<ScalarOutput> full = getScalarMetricOutputMapByLeadThreshold();
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
     * thresholds and forecast lead times. Reads the input data from {@link #getScalarMetricOutputMapByLeadThreshold()}
     * and slices.
     * 
     * @return an output map of verification scores
     */
    public static MetricOutputMapByLeadThreshold<ScalarOutput> getMetricOutputMapByLeadThresholdTwo()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByLeadThreshold<ScalarOutput> full = getScalarMetricOutputMapByLeadThreshold();
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
    private static MetricOutputMapByLeadThreshold<ScalarOutput> getScalarMetricOutputMapByLeadThreshold()
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
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link VectorOutput} comprising the CRPSS for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */
    private static MetricOutputMapByLeadThreshold<VectorOutput> getVectorMetricOutputMapByLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<MapBiKey<Integer, Threshold>, VectorOutput> rawData = new TreeMap<>();

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
                    final VectorOutput value = outputFactory.ofVectorOutput(res, meta);

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
                        for(int i = 0; i < res.length; i++)
                        {
                            for(int j = 0; j < res[i].length; j++)
                            {
                                if(res[i][j] == -999D)
                                {
                                    res[i][j] = Double.NaN;
                                }

                                //HDH (8/15/17): Forcing a NaN in the first time series within the reliability diagram at index 2.
                                if(firstOne && (i == 0) && (j == 2))
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

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link MultiVectorOutput} that contains the components of the
     * Relative Operating Characteristic (ROC) diagram (probability of detection and probability of false detection) for
     * various thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getROCDiagramByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */

    private static MetricOutputMapByLeadThreshold<MultiVectorOutput> getROCDiagramByLeadThreshold()
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
            final File resultFile = new File("testinput/chart2DTest/getROCDiagramByLeadThreshold.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("CMS"),
                                                                            MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                                            MetricConstants.MAIN,
                                                                            metaFactory.getDatasetIdentifier("NPTP1",
                                                                                                             "STREAMFLOW",
                                                                                                             "HEFS"));

            //Iterate through the lead times.
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
                    //Read only selected quantiles
                    if(allowed.contains(q))
                    {
                        final MapBiKey<Integer, Threshold> key = outputFactory.getMapKey((int)leadTime, q);

                        //Build the result
                        final MetricResult result = t.getResult(f);
                        final double[][] roc = ((DoubleMatrix2DResult)result).getResult().toArray();

                        //Ensure missings are NaN by brute force.
                        for(int i = 0; i < roc.length; i++)
                        {
                            for(int j = 0; j < roc[i].length; j++)
                            {
                                if(roc[i][j] == -999D)
                                {
                                    roc[i][j] = Double.NaN;
                                }
                            }
                        }

                        final Map<MetricConstants, double[]> output = new EnumMap<>(MetricConstants.class);
                        output.put(MetricConstants.PROBABILITY_OF_FALSE_DETECTION, roc[0]); //PoFD
                        output.put(MetricConstants.PROBABILITY_OF_DETECTION, roc[1]); //PoD
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
    
    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link MultiVectorOutput} that contains the components of the
     * Rank Histogram (rank position, which represents the number of gaps between ensemble members plus one) and 
     * the relative frequency of observations that fall within each gap. The results include various thresholds and 
     * forecast lead times. Reads the input data from testinput/chart2DTest/getRankHistogramByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */

    private static MetricOutputMapByLeadThreshold<MultiVectorOutput> getRankHistogramByLeadThreshold()
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
            final File resultFile = new File("testinput/chart2DTest/getRankHistogramByLeadThreshold.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("CMS"),
                                                                            MetricConstants.RANK_HISTOGRAM,
                                                                            MetricConstants.MAIN,
                                                                            metaFactory.getDatasetIdentifier("NPTP1",
                                                                                                             "STREAMFLOW",
                                                                                                             "HEFS"));

            //Iterate through the lead times.
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
                    //Read only selected quantiles
                    if(allowed.contains(q))
                    {
                        final MapBiKey<Integer, Threshold> key = outputFactory.getMapKey((int)leadTime, q);

                        //Build the result
                        final MetricResult result = t.getResult(f);
                        final double[][] rh = ((DoubleMatrix2DResult)result).getResult().toArray();

                        //Ensure missings are NaN by brute force.
                        for(int i = 0; i < rh.length; i++)
                        {
                            for(int j = 0; j < rh[i].length; j++)
                            {
                                if(rh[i][j] == -999D)
                                {
                                    rh[i][j] = Double.NaN;
                                }
                            }
                        }

                        final Map<MetricConstants, double[]> output = new EnumMap<>(MetricConstants.class);
                        output.put(MetricConstants.RANK_ORDER, rh[0]); 
                        output.put(MetricConstants.OBSERVED_RELATIVE_FREQUENCY, rh[1]);
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
    
    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link MultiVectorOutput} that contains the components of the
     * Quantile-Quantile Diagram (predicted quantiles and observed quantiles) for various thresholds and forecast lead
     * times. Reads the input data from testinput/chart2DTest/getQQDiagramByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */

    private static MetricOutputMapByLeadThreshold<MultiVectorOutput> getQQDiagramByLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<MapBiKey<Integer, Threshold>, MultiVectorOutput> rawData = new TreeMap<>();
        try
        {
            //Create the input file
            final File resultFile = new File("testinput/chart2DTest/getQQDiagramByLeadThreshold.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("MILLIMETER"),
                                                                            MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                                                            MetricConstants.MAIN,
                                                                            metaFactory.getDatasetIdentifier("WGCM8",
                                                                                                             "PRECIPITATION",
                                                                                                             "HEFS"));
            //Single threshold
            final QuantileThreshold threshold = outputFactory.getQuantileThreshold(Double.NEGATIVE_INFINITY,
                                                                                   Double.NEGATIVE_INFINITY,
                                                                                   Operator.GREATER);

            //Iterate through the lead times.
            while(d.hasNext())
            {

                //Set the lead time
                final double leadTime = (Double)d.next().getKey();
                final MapBiKey<Integer, Threshold> key = outputFactory.getMapKey((int)leadTime, threshold);
                final DoubleMatrix2DResult t = (DoubleMatrix2DResult)data.getResult(leadTime);
                final double[][] qq = t.getResult().toArray();

                final Map<MetricConstants, double[]> output = new EnumMap<>(MetricConstants.class);
                output.put(MetricConstants.PREDICTED_QUANTILES, qq[0]);
                output.put(MetricConstants.OBSERVED_QUANTILES, qq[1]);
                final MultiVectorOutput value = outputFactory.ofMultiVectorOutput(output, meta);

                //Append result
                rawData.put(key, value);

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
    
    /**
     * The comparison sensitivity.
     */
    private static int IMAGE_COMPARISON_SENSITIVITY = 2;
    
    /**
     * Comparison debug output
     */
    private static boolean IMAGE_COMPARISON_DEBUG_OUTPUT = false;

    /**
     * Main line compares images with benchmarks.
     * 
     * @param args
     */
    public static void main(final String[] args)
    {
        for(final File file: FileTools.listFilesWithSuffix(new File("testoutput/chart2DTest/"), ".png"))
        {
            final File benchmarkFile = new File("testinput/chart2DTest/benchmark." + file.getName());

            try
            {
                System.out.println("");
                System.out.println("####>> Comparing " + file.getName()
                    + " ================================================");
                System.out.println("");
                FileComparisonUtilities.assertImageFileSimilarToBenchmark(file,
                                                                          benchmarkFile,
                                                                          IMAGE_COMPARISON_SENSITIVITY,
                                                                          true,
                                                                          IMAGE_COMPARISON_DEBUG_OUTPUT);
            }
            catch(final Throwable t)
            {
                System.err.println("####>> Comparison failed for " + file.getName() + " and "
                    + benchmarkFile.getName() + ". Dissimilarity file was created; see debug information for the difference numbers computed.");
            }
        }
    }

}

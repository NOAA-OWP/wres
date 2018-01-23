package wres.vis;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.hefs.utils.junit.FileComparisonUtilities;
import ohd.hseb.hefs.utils.tools.FileTools;
import wres.config.generated.PlotTypeSelection;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;

/**
 * Tests the construction of a 3D chart of metric outputs. The building of the charts and images is tested via the unit
 * tests. To compare the images with benchmarks in testinput, YOU MUST EXECUTE THIS CLASS' MAIN!
 * 
 * @author hank.herr
 * @author james.brown@hydrosolved.com
 */
public class Chart2DTestOutput
{

    //TODO Note that test1 is within the Chart2DTestInput.java unit tests.  The two unit tests need to either be completely separate
    //(different testoutput, testinput directories) or merged.  
    //
    //do the following: Keep the unit tests separate; modify scenario names (inputTest1 and outputTest1) and update the file names.
    //That way the tests can be separate but generate output and using input from same directories.

    /**
     * Generate a plot by lead time on the domain axis.
     */
    @Test
    public void test2ScalarOutput()
    {
        final String scenarioName = "test2";
        final File outputImageFile = new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" );
        outputImageFile.delete();

        //Construct some single-valued pairs
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input = Chart2DTestDataGenerator.getMetricOutputMapByLeadThresholdOne();

        try
        {

            //Call the factory.
            final Map<MetricConstants, ChartEngine> engine = ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                                                             DefaultDataFactory.getInstance(),
                                                                                                             PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            ChartTools.generateOutputImageFile( outputImageFile,
                                                engine.values().iterator().next().buildChart(),
                                                800,
                                                600 );
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generate a plot by threshold on the domain axis.
     */
    @Test
    public void test3ScalarOutput()
    {
        final String scenarioName = "test3";
        final File outputImageFile = new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" );
        outputImageFile.delete();

        //Construct some single-valued pairs
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input = Chart2DTestDataGenerator.getMetricOutputMapByLeadThresholdTwo();

        try
        {

            //Call the factory.
            final Map<MetricConstants, ChartEngine> engine = ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                                                             DefaultDataFactory.getInstance(),
                                                                                                             PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            ChartTools.generateOutputImageFile( outputImageFile,
                                                engine.values().iterator().next().buildChart(),
                                                800,
                                                600 );
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates multiple reliability diagrams, one for each lead time.
     */
    @Test
    public void test4ReliabilityDiagramByLeadTime()
    {
        final String scenarioName = "test4";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getReliabilityDiagramByLeadThreshold();

//DEBUG OUTPUT:
//        results42.forEach((key,result)-> {
//            System.out.println(key.getSecondKey());
//            System.out.println(Arrays.toString(result.get(MetricConstants.FORECAST_PROBABILITY).getDoubles()));  //This array forms the domain
//            System.out.println(Arrays.toString(result.get(MetricConstants.OBSERVED_RELATIVE_FREQUENCY).getDoubles()));  //This array forms the range for reliability diagram subplot
//            System.out.println(Arrays.toString(result.get(MetricConstants.SAMPLE_SIZE).getDoubles())); //This array forms the range for the sample size subplot
//        });

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            for ( final Object lead : engineMap.keySet() )
            {
                Object key = ( (TimeWindow) lead ).getLatestLeadTimeInHours();
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                              + "h."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( lead ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates multiple reliability diagrams, one for each threshold.
     */
    @Test
    public void test5ReliabilityDiagramByThreshold()
    {
        final String scenarioName = "test5";
        final String outputImageFileSuffix = scenarioName + "_output.png";
        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getReliabilityDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            for ( final Object thresh : engineMap.keySet() )
            {
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/"
                                                              + ( (Threshold) thresh ).getThreshold()
                                                              + "."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( thresh ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates multiple plots, one for each vector index, by calling the scalar plots repeatedly.
     */
    @Test
    public void test6ScoreMetricOutput()
    {
        final String scenarioName = "test6";
        final String outputImageFileSuffix = scenarioName + "_output.png";
        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        //Construct some single-valued pairs
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input = Chart2DTestDataGenerator.getScoreMetricOutputMapByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final ConcurrentMap<MetricConstants, ChartEngine> engineMap = ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                                                                  factory,
                                                                                                                  PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                                  null,
                                                                                                                  null );
            //Generate the output file.
            for ( final Object key : engineMap.keySet() )
            {
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                              + "."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( key ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a ROC diagram by lead time.
     */
    
    @Test
    public void test7ROCDiagramByLeadTime()
    {
        final String scenarioName = "test7";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getROCDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            for ( final Object lead : engineMap.keySet() )
            {
                Object key = ( (TimeWindow) lead ).getLatestLeadTimeInHours();
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                              + "h."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( lead ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a ROC diagram by threshold.
     */
    @Test
    public void test8ROCDiagramByThreshold()
    {
        final String scenarioName = "test8";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getROCDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            for ( final Object thresh : engineMap.keySet() )
            {
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/"
                                                              + ( (Threshold) thresh ).getThreshold()
                                                              + "."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( thresh ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a QQ diagram by lead time.
     */
    @Test
    public void test9QQDiagramByLeadTime()
    {
        final String scenarioName = "test9";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getQQDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            for ( final Object lead : engineMap.keySet() )
            {
                Object key = ( (TimeWindow) lead ).getLatestLeadTimeInHours();
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                              + "h."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( lead ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a QQ diagram by threshold.
     */
    @Test
    public void test10QQDiagramByThreshold()
    {
        final String scenarioName = "test10";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getQQDiagramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            for ( final Object thresh : engineMap.keySet() )
            {
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + "alldata"
                                                              + "."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( thresh ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a rank histogram by lead time.
     */
    @Test
    public void test11RankHistogramByLeadtime()
    {
        final String scenarioName = "test11";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getRankHistogramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.LEAD_THRESHOLD,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            for ( final Object lead : engineMap.keySet() )
            {
                Object key = ( (TimeWindow) lead ).getLatestLeadTimeInHours();
                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                              + "h."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( lead ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a rank histogram by threshold.
     */
    
    @Test
    public void test12RankHistogramByThreshold()
    {
        final String scenarioName = "test12";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> results = Chart2DTestDataGenerator.getRankHistogramByLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildMultiVectorOutputChartEngine( results,
                                                                                                             factory,
                                                                                                             PlotTypeSelection.THRESHOLD_LEAD,
                                                                                                             null,
                                                                                                             null );

            for ( final Object thresh : engineMap.keySet() )
            {
                String thresholdString = ( ( (Threshold) thresh ).getThreshold() ).toString();
                if ( Double.isInfinite( ( (Threshold) thresh ).getThreshold() ) )
                {
                    thresholdString = "alldata";
                }

                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/"
                                                              + thresholdString
                                                              + "."
                                                              + outputImageFileSuffix ),
                                                    engineMap.get( thresh ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a box plot against observed values by lead time.
     */
    
    @Test
    public void test13BoxPlotObsByLeadtime()
    {
        final String scenarioName = "test13";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<BoxPlotOutput> results = Chart2DTestDataGenerator.getBoxPlotErrorsByObservedAndLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
//            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Pair<TimeWindow, Threshold>, ChartEngine> engineMap = ChartEngineFactory.buildBoxPlotChartEngine( results,
                                                                                                                        null,
                                                                                                                        null );

            //Generate the output file.
            for ( final Pair<TimeWindow, Threshold> key : engineMap.keySet() )
            {
                
                long lead = key.getLeft().getEarliestLeadTimeInHours();
                Threshold thresh = key.getRight();

                String thresholdString = ( thresh.getThreshold() ).toString();
                if ( Double.isInfinite( thresh.getThreshold() ) )
                {
                    thresholdString = "alldata";
                }


                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + lead
                                                              + "h."
                                                              + thresholdString + "."
                                                              +outputImageFileSuffix ),
                                                    engineMap.get( key ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generates a box plot against forecast values by lead time.
     */
    
    @Test
    public void test14BoxPlotForecastByLeadtime()
    {
        final String scenarioName = "test14";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        try
        {
            FileTools.deleteFiles( new File( "testoutput/chart2DTest/" ), outputImageFileSuffix );
        }
        catch ( final IOException e )
        {
            fail( "Unexpected exception occurred trying to remove files: " + e.getMessage() );
        }

        final MetricOutputMapByTimeAndThreshold<BoxPlotOutput> results = Chart2DTestDataGenerator.getBoxPlotErrorsByForecastAndLeadThreshold();

        try
        {
            //Get an implementation of the factory to use for testing.
//            final DataFactory factory = DefaultDataFactory.getInstance();

            //Call the factory.
            final Map<Pair<TimeWindow, Threshold>, ChartEngine> engineMap = ChartEngineFactory.buildBoxPlotChartEngine( results,
                                                                                                   null,
                                                                                                   null );

            //Generate the output file.
            for ( final Pair<TimeWindow, Threshold> key : engineMap.keySet() )
            {
                long lead = key.getLeft().getLatestLeadTimeInHours();
                Threshold thresh = key.getRight();

                String thresholdString = ( thresh.getThreshold() ).toString();
                if ( Double.isInfinite( thresh.getThreshold() ) )
                {
                    thresholdString = "alldata";
                }

                ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + lead
                                                              + "h."
                                                              + thresholdString + "."
                                                              +outputImageFileSuffix ),
                                                    engineMap.get( key ).buildChart(),
                                                    800,
                                                    600 );

            }
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
    }

    /**
     * Generate a plot by time window on the domain axis.
     */
    @Test
    public void test15ScalarOutput()
    {
        final String scenarioName = "test15";
        final File outputImageFile = new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" );
        outputImageFile.delete();

        //Construct some single-valued pairs
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input = Chart2DTestDataGenerator.getScalarMetricOutputMapForRollingWindows();

        try
        {

            //Call the factory.
            final Map<MetricConstants, ChartEngine> engine = ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                                                             DefaultDataFactory.getInstance(),
                                                                                                             PlotTypeSelection.POOLING_WINDOW,
                                                                                                             null,
                                                                                                             null );

            //Generate the output file.
            ChartTools.generateOutputImageFile( outputImageFile,
                                                engine.values().iterator().next().buildChart(),
                                                800,
                                                600 );
        }
        catch ( final Throwable t )
        {
            t.printStackTrace();
            fail( "Unexpected exception: " + t.getMessage() );
        }
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
    public static void main( final String[] args )
    {
        System.out.println( "####>> COMPARING GENERATED UNIT TEST IMAGES..." );
        for ( final File file : FileTools.listFilesWithSuffix( new File( "testoutput/chart2DTest/" ), ".png" ) )
        {
            final File benchmarkFile = new File( "testinput/chart2DTest/benchmark." + file.getName() );

            try
            {
                System.out.println( "" );
                System.out.println( "####>> Comparing " + file.getName()
                                    + " ================================================" );
                System.out.println( "" );
                FileComparisonUtilities.assertImageFileSimilarToBenchmark( file,
                                                                           benchmarkFile,
                                                                           IMAGE_COMPARISON_SENSITIVITY,
                                                                           true,
                                                                           IMAGE_COMPARISON_DEBUG_OUTPUT );
            }
            catch ( final Throwable t )
            {
                System.err.println( "####>> Comparison failed for " + file.getName()
                                    + " and "
                                    + benchmarkFile.getName()
                                    + ". Dissimilarity file was created; see debug information for the difference numbers computed." );
            }
        }
    }

}

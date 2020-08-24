package wres.vis;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.datamodel.MetricConstants;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;

/**
 * Tests the construction of a 3D chart of metric outputs. The building of the charts and images is tested via the unit
 * tests. To compare the images with benchmarks in testinput, YOU MUST EXECUTE THIS CLASS' MAIN!
 *
 * TODO: Many of the below tests are missing assertions.
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
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test2";
        final File outputImageFile = new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" );
        outputImageFile.delete();

        //Construct some single-valued pairs
        final List<DoubleScoreStatisticOuter> input =
                Chart2DTestDataGenerator.getMetricOutputMapByLeadThresholdOne();

        //Call the factory.
        final Map<MetricConstants, ChartEngine> engine = ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                                                         GraphicShape.LEAD_THRESHOLD,
                                                                                                         null,
                                                                                                         null,
                                                                                                         ChronoUnit.HOURS );

        //Generate the output file.
        // If we ever make meaningful assertions, remove this conditional
        // see #58348
        if ( !engine.isEmpty() )
        {
            ChartTools.generateOutputImageFile( outputImageFile,
                                                engine.values().iterator().next().buildChart(),
                                                800,
                                                600 );
        }
    }

    /**
     * Generate a plot by threshold on the domain axis.
     */
    @Test
    public void test3ScalarOutput()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test3";
        final File outputImageFile = new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" );
        outputImageFile.delete();

        //Construct some single-valued pairs
        final List<DoubleScoreStatisticOuter> input =
                Chart2DTestDataGenerator.getMetricOutputMapByLeadThresholdTwo();

        //Call the factory.
        final Map<MetricConstants, ChartEngine> engine = ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                                                         GraphicShape.LEAD_THRESHOLD,
                                                                                                         null,
                                                                                                         null,
                                                                                                         ChronoUnit.HOURS );
        // If we ever make meaningful assertions, remove this conditional
        // see #58348
        if ( !engine.isEmpty() )
        {
            ChartTools.generateOutputImageFile( outputImageFile,
                                                engine.values().iterator().next().buildChart(),
                                                800,
                                                600 );
        }
    }

    /**
     * Generates multiple reliability diagrams, one for each lead time.
     */
    @Test
    public void test4ReliabilityDiagramByLeadTime()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test4";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getReliabilityDiagramByLeadThreshold();

//DEBUG OUTPUT:
//        results42.forEach((key,result)-> {
//            System.out.println(key.getSecondKey());
//            System.out.println(Arrays.toString(result.get(MetricConstants.FORECAST_PROBABILITY).getDoubles()));  //This array forms the domain
//            System.out.println(Arrays.toString(result.get(MetricConstants.OBSERVED_RELATIVE_FREQUENCY).getDoubles()));  //This array forms the range for reliability diagram subplot
//            System.out.println(Arrays.toString(result.get(MetricConstants.SAMPLE_SIZE).getDoubles())); //This array forms the range for the sample size subplot
//        });

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.LEAD_THRESHOLD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Object lead : engineMap.keySet() )
        {
            Object key = ( (TimeWindowOuter) lead ).getLatestLeadDuration().toHours();
            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                          + "h."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( lead ).buildChart(),
                                                800,
                                                600 );

        }
    }

    /**
     * Generates multiple reliability diagrams, one for each threshold.
     */
    @Test
    public void test5ReliabilityDiagramByThreshold()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test5";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getReliabilityDiagramByLeadThreshold();

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.THRESHOLD_LEAD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Object thresh : engineMap.keySet() )
        {
            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/"
                                                          + ( (OneOrTwoThresholds) thresh ).first().getValues().first()
                                                          + "."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( thresh ).buildChart(),
                                                800,
                                                600 );

        }
    }

    /**
     * Generates multiple plots, one for each vector index, by calling the scalar plots repeatedly.
     */
    @Test
    public void test6ScoreMetricOutput()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test6";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        //Construct some single-valued pairs
        final List<DoubleScoreStatisticOuter> input =
                Chart2DTestDataGenerator.getScoreMetricOutputMapByLeadThreshold();

        //Call the factory.
        final ConcurrentMap<MetricConstants, ChartEngine> engineMap =
                ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                GraphicShape.LEAD_THRESHOLD,
                                                                null,
                                                                null,
                                                                ChronoUnit.HOURS );

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

    /**
     * Generates a ROC diagram by lead time.
     */

    @Test
    public void test7ROCDiagramByLeadTime()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test7";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getROCDiagramByLeadThreshold();

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.LEAD_THRESHOLD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Object lead : engineMap.keySet() )
        {
            Object key = ( (TimeWindowOuter) lead ).getLatestLeadDuration().toHours();
            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                          + "h."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( lead ).buildChart(),
                                                800,
                                                600 );

        }
    }

    /**
     * Generates a ROC diagram by threshold.
     */
    @Test
    public void test8ROCDiagramByThreshold()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test8";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getROCDiagramByLeadThreshold();

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.THRESHOLD_LEAD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Object thresh : engineMap.keySet() )
        {
            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/"
                                                          + ( (OneOrTwoThresholds) thresh ).first().getValues().first()
                                                          + "."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( thresh ).buildChart(),
                                                800,
                                                600 );
        }
    }

    /**
     * Generates a QQ diagram by lead time.
     */
    @Test
    public void test9QQDiagramByLeadTime()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test9";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getQQDiagramByLeadThreshold();

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.LEAD_THRESHOLD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Object lead : engineMap.keySet() )
        {
            Object key = ( (TimeWindowOuter) lead ).getLatestLeadDuration().toHours();
            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                          + "h."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( lead ).buildChart(),
                                                800,
                                                600 );
        }
    }

    /**
     * Generates a QQ diagram by threshold.
     */
    @Test
    public void test10QQDiagramByThreshold()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test10";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getQQDiagramByLeadThreshold();

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.THRESHOLD_LEAD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

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

    /**
     * Generates a rank histogram by lead time.
     */
    @Test
    public void test11RankHistogramByLeadtime()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test11";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getRankHistogramByLeadThreshold();

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.LEAD_THRESHOLD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Object lead : engineMap.keySet() )
        {
            Object key = ( (TimeWindowOuter) lead ).getLatestLeadDuration().toHours();
            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + key
                                                          + "h."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( lead ).buildChart(),
                                                800,
                                                600 );

        }
    }

    /**
     * Generates a rank histogram by threshold.
     */

    @Test
    public void test12RankHistogramByThreshold()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test12";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DiagramStatisticOuter> results =
                Chart2DTestDataGenerator.getRankHistogramByLeadThreshold();

        //Call the factory.
        final Map<Object, ChartEngine> engineMap = ChartEngineFactory.buildDiagramChartEngine( results,
                                                                                               GraphicShape.THRESHOLD_LEAD,
                                                                                               null,
                                                                                               null,
                                                                                               ChronoUnit.HOURS );

        for ( final Object thresh : engineMap.keySet() )
        {
            String thresholdString = ( ( (OneOrTwoThresholds) thresh ).first().getValues().first() ).toString();
            if ( ! ( (OneOrTwoThresholds) thresh ).first().isFinite() )
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

    /**
     * Generates a box plot against observed values by lead time.
     */

    @Test
    public void test13BoxPlotObsByLeadtime()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test13";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<BoxplotStatisticOuter> results =
                Chart2DTestDataGenerator.getBoxPlotErrorsByObservedAndLeadThreshold();

        //Get an implementation of the factory to use for testing.
        // final DataFactory factory = DefaultDataFactory.getInstance();

        //Call the factory.
        final Map<Pair<TimeWindowOuter, OneOrTwoThresholds>, ChartEngine> engineMap =
                ChartEngineFactory.buildBoxPlotChartEnginePerPool( results,
                                                                   GraphicShape.DEFAULT,
                                                                   null,
                                                                   null,
                                                                   ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Pair<TimeWindowOuter, OneOrTwoThresholds> key : engineMap.keySet() )
        {

            long lead = key.getLeft().getEarliestLeadDuration().toHours();
            OneOrTwoThresholds thresh = key.getRight();

            String thresholdString = ( thresh.first().getValues().first() ).toString();
            if ( !thresh.first().isFinite() )
            {
                thresholdString = "alldata";
            }

            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + lead
                                                          + "h."
                                                          + thresholdString
                                                          + "."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( key ).buildChart(),
                                                800,
                                                600 );
        }
    }

    /**
     * Generates a box plot against forecast values by lead time.
     */

    @Test
    public void test14BoxPlotForecastByLeadtime()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test14";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<BoxplotStatisticOuter> results =
                Chart2DTestDataGenerator.getBoxPlotErrorsByForecastAndLeadThreshold();

        //Get an implementation of the factory to use for testing.
        // final DataFactory factory = DefaultDataFactory.getInstance();

        //Call the factory.
        final Map<Pair<TimeWindowOuter, OneOrTwoThresholds>, ChartEngine> engineMap =
                ChartEngineFactory.buildBoxPlotChartEnginePerPool( results,
                                                                   GraphicShape.DEFAULT,
                                                                   null,
                                                                   null,
                                                                   ChronoUnit.HOURS );

        //Generate the output file.
        for ( final Pair<TimeWindowOuter, OneOrTwoThresholds> key : engineMap.keySet() )
        {
            long lead = key.getLeft().getLatestLeadDuration().toHours();
            OneOrTwoThresholds thresh = key.getRight();

            String thresholdString = ( thresh.first().getValues().first() ).toString();
            if ( !thresh.first().isFinite() )
            {
                thresholdString = "alldata";
            }

            ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + lead
                                                          + "h."
                                                          + thresholdString
                                                          + "."
                                                          + outputImageFileSuffix ),
                                                engineMap.get( key ).buildChart(),
                                                800,
                                                600 );
        }
    }

    /**
     * Generate a plot by time window on the domain axis.
     */
    @Test
    public void test15ScalarOutputForPoolingWindow()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test15";
        final File outputImageFile = new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" );
        outputImageFile.delete();

        //Construct some single-valued pairs
        final List<DoubleScoreStatisticOuter> input =
                Chart2DTestDataGenerator.getScoreOutputForPoolingWindowsFirst();

        //Call the factory.
        final Map<MetricConstants, ChartEngine> engine =
                ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                GraphicShape.ISSUED_DATE_POOLS,
                                                                null,
                                                                null,
                                                                ChronoUnit.HOURS );

        //Generate the output file.
        ChartTools.generateOutputImageFile( outputImageFile,
                                            engine.values().iterator().next().buildChart(),
                                            800,
                                            600 );
    }

    /**
     * Generates a ROC diagram by threshold.
     */
    @Test
    public void test16TimeToPeakErrors()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test16";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DurationDiagramStatisticOuter> input =
                Chart2DTestDataGenerator.getTimeToPeakErrors();

        //Call the factory.
        final ChartEngine engine = ChartEngineFactory.buildDurationDiagramChartEngine( input,
                                                                                       GraphicShape.DEFAULT,
                                                                                       null,
                                                                                       null,
                                                                                       ChronoUnit.HOURS );

        //Generate the output file.
        ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" +
                                                      outputImageFileSuffix ),
                                            engine.buildChart(),
                                            800,
                                            600 );
    }

    /**
     * Generates a ROC diagram by threshold.
     */
    @Test
    public void test17TimeToPeakSummaryStats()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test17";
        final String outputImageFileSuffix = scenarioName + "_output.png";

        Path pathToDelete = Paths.get( "testoutput/chart2DTest/", outputImageFileSuffix );
        Files.deleteIfExists( pathToDelete );

        final List<DurationScoreStatisticOuter> input =
                Chart2DTestDataGenerator.getTimeToPeakErrorStatistics();

        //Call the factory.
        final ChartEngine engine = ChartEngineFactory.buildCategoricalDurationScoreChartEngine( input,
                                                                                                GraphicShape.DEFAULT,
                                                                                                null,
                                                                                                null,
                                                                                                ChronoUnit.HOURS );

        //Generate the output file.
        ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" +
                                                      outputImageFileSuffix ),
                                            engine.buildChart(),
                                            800,
                                            600 );
    }

    /**
     * Generate a plot by time window on the domain axis.
     */
    @Test
    public void test18ScalarOutputForPoolingWindow()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final String scenarioName = "test18";
        final File outputImageFile = new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" );
        outputImageFile.delete();

        //Construct some single-valued pairs
        final List<DoubleScoreStatisticOuter> input =
                Chart2DTestDataGenerator.getScoreOutputForPoolingWindowsSecond();

        //Call the factory.
        final Map<MetricConstants, ChartEngine> engine =
                ChartEngineFactory.buildScoreOutputChartEngine( input,
                                                                GraphicShape.ISSUED_DATE_POOLS,
                                                                null,
                                                                null,
                                                                ChronoUnit.HOURS );

        //Generate the output file.
        ChartTools.generateOutputImageFile( outputImageFile,
                                            engine.values().iterator().next().buildChart(),
                                            800,
                                            600 );
    }

}

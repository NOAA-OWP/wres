package wres.vis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JPanel;

import junit.framework.TestCase;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartPanelTools;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;

public class Chart2DTestInput extends TestCase
{
    public void test1SingleValuedPairsScatter()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final Random rand = new Random(0L);

        final List<SingleValuedPair> values = new ArrayList<>();
        for (int i = 0; i < 100; i ++)
        {
            values.add(SingleValuedPair.of(rand.nextGaussian(), rand.nextGaussian()));
        }

        final SampleMetadata meta = SampleMetadata.of(MeasurementUnit.of("CMS"),
                                                 DatasetIdentifier.of(Location.of("DRRC2"), "SQIN", "HEFS"));
        final SingleValuedPairs pairs = SingleValuedPairs.of(values, meta);

        //Construct the source from the pairs assigning it a data source order index of 0.
        //The order index indicates the order in which the different sources are rendered.

        final String scenarioName = "test1";
        //Build the ChartEngine instance.
        final ChartEngine engine = ChartEngineFactory.buildSingleValuedPairsChartEngine( pairs,
                                                                                         "singleValuedPairsTemplate.xml",
                                                                                         null );

        //Generate the output file.
        ChartTools.generateOutputImageFile( new File( "testoutput/chart2DTest/" + scenarioName + "_output.png" ),
                                            engine.buildChart(),
                                            800,
                                            500 );

            //Compare against OS specific image benchmark.
//Turned off because this often fails.
//            FileComparisonUtilities.assertImageFileSimilarToBenchmark(new File("testoutput/chart2DTest/" + scenarioName
//                + "_output.png"),
//                                                                      new File("testinput/chart2DTest/benchmark."
//                                                                          + scenarioName + "_output.png"),
//                                                                      8,
//                                                                      true,
//                                                                      false);

    }

    public static void main( final String[] args ) throws ChartEngineException, IOException
    {
        //Construct some single-valued pairs
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add(SingleValuedPair.of(22.9, 22.8));
        values.add(SingleValuedPair.of(75.2, 80));
        values.add(SingleValuedPair.of(63.2, 65));
        values.add(SingleValuedPair.of(29, 30));
        values.add(SingleValuedPair.of(5, 2));
        values.add(SingleValuedPair.of(2.1, 3.1));
        values.add(SingleValuedPair.of(35000, 37000));
        values.add(SingleValuedPair.of(8, 7));
        values.add(SingleValuedPair.of(12, 12));
        values.add(SingleValuedPair.of(93, 94));

        final SampleMetadata meta = SampleMetadata.of(MeasurementUnit.of("CMS"),
                                                 DatasetIdentifier.of(Location.of("DRRC2"), "SQIN", "HEFS"));
        final SingleValuedPairs pairs = SingleValuedPairs.of(values, meta);

        ChartEngine engine = ChartEngineFactory.buildSingleValuedPairsChartEngine( pairs, null, null );

        //Put chart in a frame.
        final JPanel panel = ChartPanelTools.buildPanelFromChartEngine(engine, false);
        final JFrame frame = new JFrame();
        frame.setContentPane(panel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 500);
        frame.setVisible(true);
    }

    /**
     * This makes use of some James EVS stuff to read back in computed EVS results. However, we have no class for
     * storing these results in a wres-datamodel manner.
     */
//    public void test2JamesStuff() throws IOException
//    {
//        final MetricResultByLeadTime results =
//                ProductFileIO.read(new File("testinput/chart2DTest/WGCM8.Streamflow.GEFS_CFSv2_1D_ALL.Correlation_coefficient.xml"));
//            System.out.println(results.toString());
//
//    }

}

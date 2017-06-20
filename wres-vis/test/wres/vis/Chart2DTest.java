package wres.vis;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;

import com.google.common.collect.Lists;

import junit.framework.TestCase;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartPanelTools;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.hefs.utils.junit.FileComparisonUtilities;
import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubles;
import wres.engine.statistics.metric.inputs.MetricInputFactory;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;

public class Chart2DTest extends TestCase
{
    public void test1SingleValuedPairsScatter()
    {
        //Construct some single-valued pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory dataFactory = DataFactory.instance();
        values.add(dataFactory.pairOf(22.9, 22.8));
        values.add(dataFactory.pairOf(75.2, 80));
        values.add(dataFactory.pairOf(63.2, 65));
        values.add(dataFactory.pairOf(29, 30));
        values.add(dataFactory.pairOf(5, 2));
        values.add(dataFactory.pairOf(2.1, 3.1));
        values.add(dataFactory.pairOf(35000, 37000));
        values.add(dataFactory.pairOf(8, 7));
        values.add(dataFactory.pairOf(12, 12));
        values.add(dataFactory.pairOf(93, 94));
        final SingleValuedPairs pairs = MetricInputFactory.ofSingleValuedPairs(values, null);

        //Construct the source from the pairs assigning it a data source order index of 0.  
        //The order index indicates the order in which the different sources are rendered.
        final MetricInputXYChartDataSource source = new MetricInputXYChartDataSource(0, pairs);
        
        final String scenarioName = "test1";
        try
        {
            //The arguments processor for example purposes.
            final WRESArgumentProcessor arguments = new WRESArgumentProcessor();
            arguments.addArgument("locationId", "AAAAA");

            //Build the ChartEngine instance.
            final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(source),
                                                                   arguments,
                                                                   "testinput/chart2DTest/" + scenarioName
                                                                       + "_template.xml",
                                                                   null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/test1_output.png"),
                                               engine.buildChart(),
                                               800,
                                               500);

            //Compare against OS specific image benchmark.
            FileComparisonUtilities.assertImageFileSimilarToBenchmark(new File("testoutput/chart2DTest/"
                + scenarioName
                + "_output.png"), new File("testinput/chart2DTest/benchmark." + scenarioName + "_output.png"), 8, true, false);
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    public static void main(final String[] args)
    {
        //Construct some single-valued pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory dataFactory = DataFactory.instance();
        values.add(dataFactory.pairOf(22.9, 22.8));
        values.add(dataFactory.pairOf(75.2, 80));
        values.add(dataFactory.pairOf(63.2, 65));
        values.add(dataFactory.pairOf(29, 30));
        values.add(dataFactory.pairOf(5, 2));
        values.add(dataFactory.pairOf(2.1, 3.1));
        values.add(dataFactory.pairOf(35000, 37000));
        values.add(dataFactory.pairOf(8, 7));
        values.add(dataFactory.pairOf(12, 12));
        values.add(dataFactory.pairOf(93, 94));
        final SingleValuedPairs pairs = MetricInputFactory.ofSingleValuedPairs(values, null);

        //Create the data source for charting.
        final MetricInputXYChartDataSource source = new MetricInputXYChartDataSource(0, pairs);

        try
        {
            //The arguments processor for example purposes.
            final WRESArgumentProcessor arguments = new WRESArgumentProcessor();
            arguments.addArgument("locationId", "AAAAA");

            //Build the ChartEngine instance.
            final ChartEngine engine = ChartTools.buildChartEngine(Lists.newArrayList(source),
                                                                   arguments,
                                                                   "testinput/chart2DTest/test1_template.xml",
                                                                   null);

            //Put chart in a frame.
            final JPanel panel = ChartPanelTools.buildPanelFromChartEngine(engine, false);
            final JFrame frame = new JFrame();
            frame.setContentPane(panel);
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setSize(800, 500);
            frame.setVisible(true);
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            fail("Unexpected exception: " + t.getMessage());
        }
    }

    /**
     * This makes use of some James EVS stuff to read back in computed EVS results. However, we have no class for
     * storing these results in a wres-datamodel manner.
     */
//    public void test2JamesStuff()
//    {
//        try
//        {
//            final MetricResultByLeadTime results =
//                                                 ProductFileIO.read(new File("testinput/chart2DTest/WGCM8.Streamflow.GEFS_CFSv2_1D_ALL.Correlation_coefficient.xml"));
//            System.out.println(results.toString());
//        }
//        catch(final IOException e)
//        {
//            e.printStackTrace();
//            fail("Unexpected exception reading EVS output.");
//        }
//
//    }
}

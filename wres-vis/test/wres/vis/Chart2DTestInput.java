package wres.vis;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JPanel;

import com.google.common.collect.Lists;

import junit.framework.TestCase;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartPanelTools;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.hefs.utils.junit.FileComparisonUtilities;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

public class Chart2DTestInput extends TestCase
{
    public void test1SingleValuedPairsScatter()
    {
        final Random rand = new Random(0L);
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        for (int i = 0; i < 100; i ++)
        {
            values.add(metIn.pairOf(rand.nextGaussian(), rand.nextGaussian()));
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final SingleValuedPairs pairs = metIn.ofSingleValuedPairs(values, meta);
        
        //TODO Ideas...
        //Data identifier should be in the legend entry.
        //Title should be basic text that does not vary by scatter plot.
        //Series should be colored according to palette.

        //Construct the source from the pairs assigning it a data source order index of 0.  
        //The order index indicates the order in which the different sources are rendered.

        final String scenarioName = "test1";
        try
        {
            //Build the ChartEngine instance.
            final ChartEngine engine = ChartEngineFactory.buildSingleValuedPairsChartEngine(pairs, 
                                                                   "singleValuedPairsTemplate.xml",
                                                                   null);

            //Generate the output file.
            ChartTools.generateOutputImageFile(new File("testoutput/chart2DTest/test1_output.png"),
                                               engine.buildChart(),
                                               800,
                                               500);

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

    public static void main(final String[] args)
    {
        //Construct some single-valued pairs
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(22.9, 22.8));
        values.add(metIn.pairOf(75.2, 80));
        values.add(metIn.pairOf(63.2, 65));
        values.add(metIn.pairOf(29, 30));
        values.add(metIn.pairOf(5, 2));
        values.add(metIn.pairOf(2.1, 3.1));
        values.add(metIn.pairOf(35000, 37000));
        values.add(metIn.pairOf(8, 7));
        values.add(metIn.pairOf(12, 12));
        values.add(metIn.pairOf(93, 94));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final SingleValuedPairs pairs = metIn.ofSingleValuedPairs(values, meta);

        //Create the data source for charting.
        final SingleValuedPairsXYChartDataSource source = new SingleValuedPairsXYChartDataSource(0, pairs);

        try
        {
            //The arguments processor for example purposes.
            final WRESArgumentProcessor arguments = new WRESArgumentProcessor(pairs.getMetadata());
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

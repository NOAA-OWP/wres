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
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartPanelTools;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.parameters.ChartDrawingParameters;
import ohd.hseb.hefs.utils.junit.FileComparisonUtilities;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import ohd.hseb.hefs.utils.xml.XMLTools;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DatasetIdentifier;
import wres.datamodel.metric.DefaultMetadataFactory;
import wres.datamodel.metric.DefaultMetricInputFactory;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricInputFactory;
import wres.datamodel.metric.SingleValuedPairs;

public class Chart2DTestInput extends TestCase
{
    public void test1SingleValuedPairsScatter()
    {
        final Random rand = new Random(0L);
        final MetricInputFactory metIn = DefaultMetricInputFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        for (int i = 0; i < 100; i ++)
        {
            values.add(metIn.pairOf(rand.nextGaussian(), rand.nextGaussian()));
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(values.size(),
                                                 metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final SingleValuedPairs pairs = metIn.ofSingleValuedPairs(values, meta);
        
        //TODO Ideas...
        //Data identifier should be in the legend entry.
        //Title should be basic text that does not vary by scatter plot.
        //Series should be colored according to palette.

        //Construct the source from the pairs assigning it a data source order index of 0.  
        //The order index indicates the order in which the different sources are rendered.
        final MetricInputXYChartDataSource source = new MetricInputXYChartDataSource(0, pairs);

        final String scenarioName = "test1";
        try
        {
            //Build the ChartEngine instance.
            final ChartEngine engine = buildSingleValuedPairsChartEngine(pairs, 
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
                                                                      4,
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
        final MetricInputFactory metIn = DefaultMetricInputFactory.getInstance();
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
        final Metadata meta = metFac.getMetadata(values.size(),
                                                 metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final SingleValuedPairs pairs = metIn.ofSingleValuedPairs(values, meta);

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
    
    
    
    
    public static ChartEngine buildSingleValuedPairsChartEngine(final SingleValuedPairs input,
                                                                  final String templateResourceName,
                                                                  final String overrideParametersStr) throws ChartEngineException,
                                                                                                      GenericXMLReadingHandlerException
    {
        //Build the source.
        final MetricInputXYChartDataSource source = new MetricInputXYChartDataSource(0, input);

        //Setup the arguments.
        final WRESArgumentProcessor arguments = new WRESArgumentProcessor();
        
        //The following helper factory is part of the wres-datamodel, not the api. It will need to be supplied by 
        //(i.e. dependency injected from) wres-core as a MetadataFactory, which is part of the API
        final MetadataFactory factory = DefaultMetadataFactory.getInstance();
        final Metadata meta = input.getMetadata();
        final DatasetIdentifier identifier = meta.getIdentifier();

        //Setup fixed arguments.
        arguments.addArgument("locationName", identifier.getGeospatialID());
        arguments.addArgument("variableName", identifier.getVariableID());
        arguments.addArgument("rangeAxisLabelPrefix", "Forecast");
        arguments.addArgument("domainAxisLabelPrefix",  "Observed");
        arguments.addArgument("primaryScenario", identifier.getScenarioID());
        arguments.addArgument("inputUnitsText", " [" + meta.getDimension() + "]");

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

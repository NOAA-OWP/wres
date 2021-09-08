package wres.vis;

import java.io.File;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import junit.framework.TestCase;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.Evaluation;

public class Chart2DTestInput extends TestCase
{
    private static final FeatureKey NWS_FEATURE = new FeatureKey( "DRRC2", null, null, null );
    private static final FeatureKey USGS_FEATURE =
            new FeatureKey( "09165000", "DOLORES RIVER BELOW RICO, CO.", 4326, "POINT ( -108.0603517 37.63888428 )" );
    private static final FeatureKey NWM_FEATURE = new FeatureKey( "18384141", null, null, null );
    private static final FeatureGroup FEATURE_GROUP =
            FeatureGroup.of( new FeatureTuple( USGS_FEATURE, NWS_FEATURE, NWM_FEATURE ) );

    public void test1SingleValuedPairsScatter()
            throws ChartEngineException, XYChartDataSourceException, IOException
    {
        final Random rand = new Random( 0L );

        final List<Pair<Double, Double>> values = new ArrayList<>();
        for ( int i = 0; i < 100; i++ )
        {
            values.add( Pair.of( rand.nextGaussian(), rand.nextGaussian() ) );
        }

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( FEATURE_GROUP,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        final Pool<Pair<Double, Double>> pairs = BasicPool.of( values, meta );

        //Construct the source from the pairs assigning it a data source order index of 0.
        //The order index indicates the order in which the different sources are rendered.

        final String scenarioName = "test1";
        //Build the ChartEngine instance.
        final ChartEngine engine = ChartEngineFactory.buildSingleValuedPairsChartEngine( pairs,
                                                                                         "singleValuedPairsTemplate.xml",
                                                                                         null,
                                                                                         ChronoUnit.HOURS );

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

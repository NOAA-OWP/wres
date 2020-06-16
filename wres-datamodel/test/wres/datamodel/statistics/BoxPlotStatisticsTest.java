package wres.datamodel.statistics;

import static org.junit.Assert.assertTrue;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Tests the {@link BoxPlotStatistics}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class BoxPlotStatisticsTest
{

    /**
     * Basic statistic.
     */

    private BoxPlotStatistics basic = null;

    @Before
    public void runBeforeEachClass()
    {
        Location l2 = FeatureKey.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                     MetricConstants.MAIN );

        List<BoxPlotStatistic> mva = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( BoxPlotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
        }

        basic = BoxPlotStatistics.of( mva, m1 );
    }

    /**
     * Constructs a {@link BoxPlotStatistics} and tests for equality with another {@link BoxPlotStatistics}.
     */

    @Test
    public void testEquals()
    {

        //Build datasets
        final Location l1 = FeatureKey.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );
        final Location l2 = FeatureKey.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           12,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );
        final Location l3 = FeatureKey.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );
        List<BoxPlotStatistic> mva = new ArrayList<>();
        List<BoxPlotStatistic> mvb = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( BoxPlotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
            mvb.add( BoxPlotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
        }

        List<BoxPlotStatistic> mvc = new ArrayList<>();
        VectorOfDoubles pb = VectorOfDoubles.of( 0.0, 0.25, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( BoxPlotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }
        List<BoxPlotStatistic> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( BoxPlotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }
        List<BoxPlotStatistic> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( BoxPlotStatistic.of( pb,
                                          VectorOfDoubles.of( 2, 3, 4, 5 ),
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }

        List<BoxPlotStatistic> mvf = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvf.add( BoxPlotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          m1,
                                          1,
                                          MetricDimension.ENSEMBLE_MEAN ) );
        }
        List<BoxPlotStatistic> mvg = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvg.add( BoxPlotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          MetricDimension.FORECAST_VALUE,
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }

        final BoxPlotStatistics q =
                BoxPlotStatistics.of( mva, m2 );
        final BoxPlotStatistics r =
                BoxPlotStatistics.of( mvb, m3 );
        final BoxPlotStatistics s =
                BoxPlotStatistics.of( mva, m1 );
        final BoxPlotStatistics t =
                BoxPlotStatistics.of( mvb, m1 );
        final BoxPlotStatistics u =
                BoxPlotStatistics.of( mvc, m1 );
        final BoxPlotStatistics v =
                BoxPlotStatistics.of( mvc, m2 );
        final BoxPlotStatistics w =
                BoxPlotStatistics.of( mvd, m2 );
        final BoxPlotStatistics x =
                BoxPlotStatistics.of( mvf, m2 );
        final BoxPlotStatistics y =
                BoxPlotStatistics.of( mvg, m2 );
        final BoxPlotStatistics z =
                BoxPlotStatistics.of( mve, m2 );

        // Compare
        assertThat( s, is( t ) );
        assertThat( null, not( s ) );
        assertThat( s, not( Double.valueOf( 1.0 ) ) );
        assertThat( s, not( u ) );
        assertThat( s, not( v ) );
        assertThat( v, not( w ) );
        assertThat( w, not( x ) );
        assertThat( w, not( y ) );
        assertThat( w, not( z ) );
        assertThat( q, is( q ) );
        assertThat( s, not( q ) );
        assertThat( q, not( s ) );
        assertThat( q, not( r ) );
    }

    /**
     * Constructs a {@link BoxPlotStatistics} and tests for equal hashcodes with another {@link BoxPlotStatistics}.
     */

    @Test
    public void testHashcode()
    {
        // Equal objects have equal hashcodes
        assertThat( basic.hashCode(), is( basic.hashCode() ) );

        // Consistent with equals
        Location l2 = FeatureKey.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                     MetricConstants.MAIN );

        List<BoxPlotStatistic> mva = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( BoxPlotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
        }

        BoxPlotStatistics basicTwo =
                BoxPlotStatistics.of( mva, m1 );

        assertTrue( basic.equals( basicTwo ) && basic.hashCode() == basicTwo.hashCode() );

        // Consistency
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( basic.hashCode() == basicTwo.hashCode() );
        }

    }

    /**
     * Constructs a {@link BoxPlotStatistics} and checks the {@link BoxPlotStatistics#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {

        final Location l1 = FeatureKey.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );

        assertThat( basic.getMetadata(), is( m1 ) );
    }

    /**
     * Constructs a {@link BoxPlotStatistics} and checks the accessor methods for correct operation.
     */

    @Test
    public void testAccessors()
    {
        assertTrue( basic.getData().size() == 10 );
    }


}

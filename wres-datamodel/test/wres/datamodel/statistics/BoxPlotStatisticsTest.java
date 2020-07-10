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
 * Tests the {@link BoxplotStatisticOuter}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class BoxPlotStatisticsTest
{

    /**
     * Basic statistic.
     */

    private BoxplotStatisticOuter basic = null;

    @Before
    public void runBeforeEachClass()
    {
        Location l2 = Location.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                     MetricConstants.MAIN );

        List<BoxplotStatistic> mva = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( BoxplotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
        }

        basic = BoxplotStatisticOuter.of( mva, m1 );
    }

    /**
     * Constructs a {@link BoxplotStatisticOuter} and tests for equality with another {@link BoxplotStatisticOuter}.
     */

    @Test
    public void testEquals()
    {

        //Build datasets
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           12,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );
        List<BoxplotStatistic> mva = new ArrayList<>();
        List<BoxplotStatistic> mvb = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( BoxplotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
            mvb.add( BoxplotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
        }

        List<BoxplotStatistic> mvc = new ArrayList<>();
        VectorOfDoubles pb = VectorOfDoubles.of( 0.0, 0.25, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( BoxplotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }
        List<BoxplotStatistic> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( BoxplotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }
        List<BoxplotStatistic> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( BoxplotStatistic.of( pb,
                                          VectorOfDoubles.of( 2, 3, 4, 5 ),
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }

        List<BoxplotStatistic> mvf = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvf.add( BoxplotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          m1,
                                          1,
                                          MetricDimension.ENSEMBLE_MEAN ) );
        }
        List<BoxplotStatistic> mvg = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvg.add( BoxplotStatistic.of( pb,
                                          VectorOfDoubles.of( 1, 2, 3, 4 ),
                                          MetricDimension.FORECAST_VALUE,
                                          m1,
                                          1,
                                          MetricDimension.OBSERVED_VALUE ) );
        }

        final BoxplotStatisticOuter q =
                BoxplotStatisticOuter.of( mva, m2 );
        final BoxplotStatisticOuter r =
                BoxplotStatisticOuter.of( mvb, m3 );
        final BoxplotStatisticOuter s =
                BoxplotStatisticOuter.of( mva, m1 );
        final BoxplotStatisticOuter t =
                BoxplotStatisticOuter.of( mvb, m1 );
        final BoxplotStatisticOuter u =
                BoxplotStatisticOuter.of( mvc, m1 );
        final BoxplotStatisticOuter v =
                BoxplotStatisticOuter.of( mvc, m2 );
        final BoxplotStatisticOuter w =
                BoxplotStatisticOuter.of( mvd, m2 );
        final BoxplotStatisticOuter x =
                BoxplotStatisticOuter.of( mvf, m2 );
        final BoxplotStatisticOuter y =
                BoxplotStatisticOuter.of( mvg, m2 );
        final BoxplotStatisticOuter z =
                BoxplotStatisticOuter.of( mve, m2 );

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
     * Constructs a {@link BoxplotStatisticOuter} and tests for equal hashcodes with another {@link BoxplotStatisticOuter}.
     */

    @Test
    public void testHashcode()
    {
        // Equal objects have equal hashcodes
        assertThat( basic.hashCode(), is( basic.hashCode() ) );

        // Consistent with equals
        Location l2 = Location.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                     MetricConstants.MAIN );

        List<BoxplotStatistic> mva = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( BoxplotStatistic.of( pa, VectorOfDoubles.of( 1, 2, 3 ), m1, 1, MetricDimension.OBSERVED_VALUE ) );
        }

        BoxplotStatisticOuter basicTwo =
                BoxplotStatisticOuter.of( mva, m1 );

        assertTrue( basic.equals( basicTwo ) && basic.hashCode() == basicTwo.hashCode() );

        // Consistency
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( basic.hashCode() == basicTwo.hashCode() );
        }

    }

    /**
     * Constructs a {@link BoxplotStatisticOuter} and checks the {@link BoxplotStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {

        final Location l1 = Location.of( "A" );
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
     * Constructs a {@link BoxplotStatisticOuter} and checks the accessor methods for correct operation.
     */

    @Test
    public void testAccessors()
    {
        assertTrue( basic.getData().size() == 10 );
    }


}

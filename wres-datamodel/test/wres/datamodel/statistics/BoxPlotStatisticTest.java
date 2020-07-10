package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;

import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Tests the {@link BoxplotStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class BoxPlotStatisticTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Basic instance for testing.
     */

    private BoxplotStatistic bpa = null;

    @Before
    public void beforeEachTest()
    {
        //Build a statistic
        final Location locA = Location.of( "A" );
        final StatisticMetadata ma = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locA,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS,
                                                           MetricConstants.MAIN );

        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        VectorOfDoubles qa = VectorOfDoubles.of( 1, 2, 3 );
        double la = 1;
        bpa = BoxplotStatistic.of( pa, qa, ma, la, MetricDimension.OBSERVED_VALUE );
    }

    /**
     * Tests the {@link BoxplotStatistic#equals(Object)}.
     */

    @Test
    public void testEquals()
    {

        final Location locA = Location.of( "A" );
        final StatisticMetadata ma = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locA,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS,
                                                           MetricConstants.MAIN );

        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        VectorOfDoubles qa = VectorOfDoubles.of( 1, 2, 3 );
        double la = 1;

        // Reflexive 
        assertEquals( bpa, bpa );

        // Symmetric
        BoxplotStatistic bpb = BoxplotStatistic.of( pa, qa, ma, la, MetricDimension.OBSERVED_VALUE );

        assertTrue( bpa.equals( bpb ) && bpb.equals( bpa ) );

        // Transitive
        BoxplotStatistic bpc = BoxplotStatistic.of( pa, qa, ma, la, MetricDimension.OBSERVED_VALUE );

        assertTrue( bpa.equals( bpc ) && bpc.equals( bpb ) && bpa.equals( bpb ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( bpa.equals( bpb ) );
        }

        // Unequal cases

        // Unequal probabilities        
        VectorOfDoubles pb = VectorOfDoubles.of( 0.0, 0.6, 1.0 );
        BoxplotStatistic bpd = BoxplotStatistic.of( pb, qa, ma, la, MetricDimension.OBSERVED_VALUE );

        assertThat( bpd, not( bpa ) );

        // Unequal quantiles
        VectorOfDoubles qb = VectorOfDoubles.of( 1, 2, 4 );
        BoxplotStatistic bpe = BoxplotStatistic.of( pa, qb, ma, la, MetricDimension.OBSERVED_VALUE );

        assertThat( bpe, not( bpa ) );

        // Unequal metadata
        final StatisticMetadata mb = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locA,
                                                                                                    "B",
                                                                                                    "E" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        BoxplotStatistic bpf = BoxplotStatistic.of( pa, qa, mb, la, MetricDimension.OBSERVED_VALUE );

        assertThat( bpf, not( bpa ) );

        // Unequal linked values
        BoxplotStatistic bpg = BoxplotStatistic.of( pa, qa, mb );
        assertThat( bpg, not( bpa ) );

        // Unequal linked value type
        BoxplotStatistic bph = BoxplotStatistic.of( pa, qa, mb, la, MetricDimension.FORECAST_VALUE );
        assertThat( bph, not( bpf ) );

        // Unequal value type
        BoxplotStatistic bpi =
                BoxplotStatistic.of( pa, qa, MetricDimension.ENSEMBLE_MEAN, mb, la, MetricDimension.FORECAST_VALUE );

        assertThat( bph, not( bpi ) );

        // Different object type       
        assertThat( bpg, not( Double.valueOf( 1 ) ) );
    }


    /**
     * Tests the {@link BoxplotStatistic#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal objects have equal hashcodes
        assertEquals( bpa.hashCode(), bpa.hashCode() );

        // Consistent with equals
        final Location locA = Location.of( "A" );
        final StatisticMetadata ma = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locA,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS,
                                                           MetricConstants.MAIN );

        VectorOfDoubles pa = VectorOfDoubles.of( 0.0, 0.5, 1.0 );
        VectorOfDoubles qa = VectorOfDoubles.of( 1, 2, 3 );
        double la = 1;
        BoxplotStatistic bpb = BoxplotStatistic.of( pa, qa, ma, la, MetricDimension.OBSERVED_VALUE );

        assertTrue( bpa.equals( bpb ) && bpb.equals( bpa ) && bpa.hashCode() == bpb.hashCode() );


        // Internally consistent      
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( bpa.hashCode() == bpb.hashCode() );
        }
    }

    /**
     * Tests the {@link BoxplotStatistic#hasLinkedValue()}
     */

    @Test
    public void testHasLinkedValue()
    {
        assertTrue( bpa.hasLinkedValue() );

        BoxplotStatistic bpb = BoxplotStatistic.of( VectorOfDoubles.of( 0.1, 0.9 ),
                                                    VectorOfDoubles.of( 1, 2 ),
                                                    StatisticMetadata.of( SampleMetadata.of(),
                                                                          10,
                                                                          MeasurementUnit.of(),
                                                                          MetricConstants.BOX_PLOT_OF_ERRORS,
                                                                          MetricConstants.MAIN ) );

        assertFalse( bpb.hasLinkedValue() );

    }

    /**
     * Tests the {@link BoxplotStatistic#getLinkedValue()}
     */

    @Test
    public void testGetLinkedValue()
    {
        assertThat( bpa.getLinkedValue(), is( Double.valueOf( 1 ) ) );
    }

    /**
     * Tests the {@link BoxplotStatistic#getLinkedValueType()}
     */

    @Test
    public void testGetLinkedValueType()
    {
        assertThat( bpa.getLinkedValueType(), is( MetricDimension.OBSERVED_VALUE ) );
    }

    /**
     * Tests the {@link BoxplotStatistic#getValueType()}
     */

    @Test
    public void testGetValueType()
    {
        assertThat( bpa.getValueType(), is( MetricDimension.FORECAST_ERROR ) );
    }

    /**
     * Tests the {@link BoxplotStatistic#toString}
     */

    @Test
    public void testToString()
    {
        assertThat( bpa.toString(),
                    is( "(PROBABILITIES: [0.0, 0.5, 1.0],QUANTILES: [1.0, 2.0, 3.0],"
                        + "VALUE TYPE: FORECAST ERROR,LINKED VALUE: 1.0,LINKED VALUE TYPE: OBSERVED VALUE)" ) );
    }

    /**
     * Tests for an expected exception on construction with a probability < 0.
     */

    @Test
    public void testBuildThrowsExceptionOnProbabilityLessThanZero()
    {
        exception.expect( StatisticException.class );
        exception.expectMessage( "One or more of the probabilities is out of bounds. Probabilities must be in [0,1]" );

        BoxplotStatistic.of( VectorOfDoubles.of( -0.1, 0.1 ),
                             VectorOfDoubles.of( 1, 2 ),
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ) );
    }


    /**
     * Tests for an expected exception on construction with a probability > 1.
     */

    @Test
    public void testBuildThrowsExceptionOnProbabilityGreaterThanOne()
    {
        exception.expect( StatisticException.class );
        exception.expectMessage( "One or more of the probabilities is out of bounds. Probabilities must be in [0,1]" );

        BoxplotStatistic.of( VectorOfDoubles.of( 0.0, 1.1 ),
                             VectorOfDoubles.of( 1, 2 ),
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ) );
    }


    /**
     * Tests for an expected exception on construction with more probabilities than quantiles.
     */

    @Test
    public void testBuildThrowsExceptionOnMoreProbabilitiesThanQuantiles()
    {
        exception.expect( StatisticException.class );
        exception.expectMessage( "The number of probabilities (3) does not match the number of quantiles (2)." );

        BoxplotStatistic.of( VectorOfDoubles.of( 0.0, 0.1, 0.2 ),
                             VectorOfDoubles.of( 1, 2 ),
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ) );
    }

    /**
     * Tests for an expected exception on construction with more quantiles than probabilities.
     */

    @Test
    public void testBuildThrowsExceptionOnMoreQuantilesThanProbabilities()
    {
        exception.expect( StatisticException.class );
        exception.expectMessage( "The number of probabilities (2) does not match the number of quantiles (3)." );

        BoxplotStatistic.of( VectorOfDoubles.of( 0.0, 0.1 ),
                             VectorOfDoubles.of( 1, 2, 3 ),
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ) );
    }

    /**
     * Tests for an expected exception on construction with null probabilities.
     */

    @Test
    public void testBuildThrowsExceptionWithNullProbabilities()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify non-null probabilities for the box plot statistic." );

        BoxplotStatistic.of( null,
                             VectorOfDoubles.of( 1, 2, 3 ),
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ) );
    }

    /**
     * Tests for an expected exception on construction with null quantiles.
     */

    @Test
    public void testBuildThrowsExceptionWithNullQuantiles()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify non-null quantiles for the box plot statistic." );

        BoxplotStatistic.of( VectorOfDoubles.of( 0.0, 0.9 ),
                             null,
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ) );
    }

    /**
     * Tests for an expected exception on construction with null metadata.
     */

    @Test
    public void testBuildThrowsExceptionWithNullMetadata()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify non-null metadata for the box plot statistic." );

        BoxplotStatistic.of( VectorOfDoubles.of( 0.0, 0.9 ),
                             VectorOfDoubles.of( 1, 2 ),
                             null );
    }

    /**
     * Tests for an expected exception on construction with null value type.
     */

    @Test
    public void testBuildThrowsExceptionWithNullValueType()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify a non-null value type for the box plot statistic." );

        BoxplotStatistic.of( VectorOfDoubles.of( 0.0, 0.9 ),
                             VectorOfDoubles.of( 1, 2 ),
                             null,
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ),
                             1,
                             MetricDimension.OBSERVED_VALUE );
    }
    
    /**
     * Tests for an expected exception on construction with null linked value type.
     */

    @Test
    public void testBuildThrowsExceptionWithNullLinkedValueType()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify a non-null linked value type for the box plot statistic." );

        BoxplotStatistic.of( VectorOfDoubles.of( 0.0, 0.9 ),
                             VectorOfDoubles.of( 1, 2 ),
                             MetricDimension.FORECAST_ERROR,
                             StatisticMetadata.of( SampleMetadata.of(),
                                                   10,
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BOX_PLOT_OF_ERRORS,
                                                   MetricConstants.MAIN ),
                             1,
                             null );
    }    
    

}

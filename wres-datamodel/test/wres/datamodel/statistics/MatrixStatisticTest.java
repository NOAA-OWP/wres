package wres.datamodel.statistics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Tests the {@link MatrixStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class MatrixStatisticTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Constructs a {@link MatrixStatistic} and tests for equality with another {@link MatrixStatistic}.
     */

    @Test
    public void testEquals()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           11,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        final MatrixStatistic s = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixStatistic t = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        assertTrue( s.equals( t ) );
        assertNotEquals( null, s );
        assertNotEquals( Double.valueOf( 1.0 ), s );
        assertFalse( s.equals( MatrixStatistic.of( new double[][] { { 2.0 }, { 1.0 } }, m1 ) ) );
        assertFalse( s.equals( MatrixStatistic.of( new double[][] { { 2.0 }, { 1.0 } }, m2 ) ) );
        final MatrixStatistic q = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m2 );
        final MatrixStatistic r = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m3 );
        final MatrixStatistic u = MatrixStatistic.of( new double[][] { { 1.0, 1.0 } }, m3 );
        final MatrixStatistic v = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 }, { 1.0 } }, m3 );
        final MatrixStatistic w = MatrixStatistic.of( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, m3 );
        assertTrue( q.equals( q ) );
        assertFalse( s.equals( q ) );
        assertFalse( q.equals( s ) );
        assertFalse( q.equals( r ) );
        assertFalse( r.equals( u ) );
        assertFalse( r.equals( v ) );
        assertFalse( r.equals( w ) );
        final MatrixStatistic x =
                MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } },
                                    Arrays.asList( MetricDimension.ENSEMBLE_MEAN,
                                                   MetricDimension.ENSEMBLE_MEDIAN ),
                                    m1 );
        final MatrixStatistic y =
                MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } },
                                    Arrays.asList( MetricDimension.ENSEMBLE_MEAN,
                                                   MetricDimension.ENSEMBLE_MEDIAN ),
                                    m1 );
        assertTrue( x.equals( y ) );
        assertFalse( x.equals( w ) );
        assertFalse( x.equals( s ) );
    }

    /**
     * Constructs a {@link MatrixStatistic} and checks the {@link MatrixStatistic#toString()} representation.
     */

    @Test
    public void testToString()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        final MatrixStatistic s = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixStatistic t = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        assertTrue( s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link MatrixStatistic} and checks the {@link MatrixStatistic#getMetadata()}.
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
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        final Location l2 = Location.of( "B" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        final MatrixStatistic q = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixStatistic r = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m2 );
        assertFalse( "Metadata equal.", q.getMetadata().equals( r.getMetadata() ) );
        assertTrue( "Metadata unequal.", q.getMetadata().equals( m1 ) );
    }

    /**
     * Constructs a {@link MatrixStatistic} and checks the {@link MatrixStatistic#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        final MatrixStatistic q = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixStatistic r = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixStatistic s =
                MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } },
                                    Arrays.asList( MetricDimension.ENSEMBLE_MEAN,
                                                   MetricDimension.ENSEMBLE_MEDIAN ),
                                    m1 );
        final MatrixStatistic t =
                MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } },
                                    Arrays.asList( MetricDimension.ENSEMBLE_MEAN,
                                                   MetricDimension.ENSEMBLE_MEDIAN ),
                                    m1 );
        assertTrue( q.hashCode() == r.hashCode() );
        assertTrue( s.hashCode() == t.hashCode() );
        assertTrue( s.getComponentNameAtIndex( 1 ) == MetricDimension.ENSEMBLE_MEDIAN );
        assertTrue( s.getComponentNames()
                     .equals( Arrays.asList( MetricDimension.ENSEMBLE_MEAN, MetricDimension.ENSEMBLE_MEDIAN ) ) );
    }

    /**
     * Constructs a {@link MatrixStatistic} and tests the {@link MatrixStatistic#getComponentAtIndex(int)}.
     */

    @Test
    public void testRowMajorIndex()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );
        final MatrixStatistic s =
                MatrixStatistic.of( new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } },
                                    m1 );
        assertTrue( "Unexpected number of elements in the maxtrix.", s.size() == 9 );
        assertFalse( "Unexpected component names in the maxtrix.", s.hasComponentNames() );
        // Test the row-major indexing
        Iterator<Double> iterator = s.iterator();
        for ( int i = 0; i < 9; i++ )
        {
            assertTrue( "Unexpected element at row-major index '" + i
                        + "'.",
                        Double.compare( i + 1.0, s.getComponentAtIndex( i ) ) == 0 );
            assertTrue( "Unexpected element at row-major index '" + i
                        + "'.",
                        Double.compare( i + 1.0, iterator.next() ) == 0 );
        }
    }

    @Test
    public void testExceptionOnConstructionWithNullData()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        exception.expect( StatisticException.class );

        MatrixStatistic.of( (MatrixOfDoubles) null, null, m1 );
    }

    @Test
    public void testExceptionsOnConstructionWithNullMetadata()
    {
        exception.expect( StatisticException.class );

        MatrixStatistic.of( new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } },
                            null );
    }

    @Test
    public void testExceptionsOnConstructionWithFewerNamesThanRequired()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        exception.expect( StatisticException.class );

        MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } },
                            Arrays.asList( MetricDimension.ENSEMBLE_MEAN ),
                            m1 );

    }

    @Test
    public void testExceptionsOnAccessToOOBIndex()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINGENCY_TABLE,
                                                           MetricConstants.MAIN );

        MatrixStatistic test = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );

        exception.expect( IndexOutOfBoundsException.class );

        test.getComponentAtIndex( 3 );

    }


}

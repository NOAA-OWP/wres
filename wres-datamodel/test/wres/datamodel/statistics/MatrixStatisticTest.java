package wres.datamodel.statistics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;

/**
 * Tests the {@link MatrixStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class MatrixStatisticTest
{

    /**
     * Constructs a {@link MatrixStatistic} and tests for equality with another {@link MatrixStatistic}.
     */

    @SuppressWarnings( "unlikely-arg-type" )
    @Test
    public void test1Equals()
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
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertFalse( "Expected unequal outputs.", s.equals( null ) );
        assertFalse( "Expected unequal outputs.", s.equals( Double.valueOf( 1.0 ) ) );
        assertFalse( "Expected unequal outputs.",
                     s.equals( MatrixStatistic.of( new double[][] { { 2.0 }, { 1.0 } }, m1 ) ) );
        assertFalse( "Expected unequal outputs.",
                     s.equals( MatrixStatistic.of( new double[][] { { 2.0 }, { 1.0 } }, m2 ) ) );
        final MatrixStatistic q = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m2 );
        final MatrixStatistic r = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m3 );
        final MatrixStatistic u = MatrixStatistic.of( new double[][] { { 1.0, 1.0 } }, m3 );
        final MatrixStatistic v = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 }, { 1.0 } }, m3 );
        final MatrixStatistic w = MatrixStatistic.of( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, m3 );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertFalse( "Expected unequal outputs.", s.equals( q ) );
        assertFalse( "Expected unequal outputs.", q.equals( s ) );
        assertFalse( "Expected unequal outputs.", q.equals( r ) );
        assertFalse( "Expected unequal outputs.", r.equals( u ) );
        assertFalse( "Expected unequal outputs.", r.equals( v ) );
        assertFalse( "Expected unequal outputs.", r.equals( w ) );
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
        assertTrue( "Expected equal outputs.", x.equals( y ) );
        assertFalse( "Expected unequal outputs.", x.equals( w ) );
        assertFalse( "Expected unequal outputs.", x.equals( s ) );
    }

    /**
     * Constructs a {@link MatrixStatistic} and checks the {@link MatrixStatistic#toString()} representation.
     */

    @Test
    public void test2ToString()
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
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link MatrixStatistic} and checks the {@link MatrixStatistic#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
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
    public void test4HashCode()
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
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected equal hash codes.", s.hashCode() == t.hashCode() );
        assertTrue( "Wrong component name at index '1'",
                    s.getComponentNameAtIndex( 1 ) == MetricDimension.ENSEMBLE_MEDIAN );
        assertTrue( "Wrong component names.",
                    s.getComponentNames()
                     .equals( Arrays.asList( MetricDimension.ENSEMBLE_MEAN, MetricDimension.ENSEMBLE_MEDIAN ) ) );
    }

    /**
     * Constructs a {@link MatrixStatistic} and tests the {@link MatrixStatistic#getComponentAtIndex(int)}.
     */

    @Test
    public void test5RowMajorIndex()
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
                        Double.compare( i + 1, s.getComponentAtIndex( i ) ) == 0 );
            assertTrue( "Unexpected element at row-major index '" + i
                        + "'.",
                        Double.compare( i + 1, iterator.next() ) == 0 );
        }
    }

    /**
     * Tests the exceptional cases associated with {@link MatrixStatistic}.
     */

    @Test
    public void test6Exceptions()
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
        // Null raw data
        try
        {
            MatrixStatistic.of( (MatrixOfDoubles) null, null, m1 );
            fail( "Expected an exception on attempting to construct a matrix output with null input." );
        }
        catch ( StatisticException e )
        {
        }
        // Null metadata 
        try
        {
            MatrixStatistic.of( new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } },
                                        null );
            fail( "Expected an exception on attempting to construct a matrix output with null metadata." );
        }
        catch ( StatisticException e )
        {
        }
        // Wrong number of names
        try
        {
            MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } },
                                        Arrays.asList( MetricDimension.ENSEMBLE_MEAN ),
                                        m1 );
            fail( "Expected an exception on attempting to construct a matrix output with fewer named components than "
                  + "requrieDataFactory." );
        }
        catch ( StatisticException e )
        {
        }
        // Attempting to access an incorrect index
        try
        {
            MatrixStatistic test = MatrixStatistic.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
            test.getComponentAtIndex( 3 );
            fail( "Expected an exception on attempting to access an incorrect index." );
        }
        catch ( IndexOutOfBoundsException e )
        {
        }
        // Attempting to access an incorrect index
        try
        {
            MatrixStatistic test = MatrixStatistic.of( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, m1 );
            test.getComponentAtIndex( 4 );
            fail( "Expected an exception on attempting to access an incorrect index." );
        }
        catch ( IndexOutOfBoundsException e )
        {
        }
    }


}

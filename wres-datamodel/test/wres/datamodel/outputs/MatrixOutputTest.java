package wres.datamodel.outputs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * Tests the {@link MatrixOutput}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class MatrixOutputTest
{

    /**
     * Constructs a {@link MatrixOutput} and tests for equality with another {@link MatrixOutput}.
     */

    @Test
    public void test1Equals()
    {
        final Location l1 = MetadataFactory.getLocation( "A" );
        final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = MetadataFactory.getLocation( "A" );
        final MetricOutputMetadata m2 = MetadataFactory.getOutputMetadata( 11,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l3 = MetadataFactory.getLocation( "B" );
        final MetricOutputMetadata m3 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l3,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final MatrixOutput s = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput t = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertFalse( "Expected unequal outputs.", s.equals( null ) );
        assertFalse( "Expected unequal outputs.", s.equals( new Double( 1.0 ) ) );
        assertFalse( "Expected unequal outputs.",
                     s.equals( MatrixOutput.of( new double[][] { { 2.0 }, { 1.0 } }, m1 ) ) );
        assertFalse( "Expected unequal outputs.",
                     s.equals( MatrixOutput.of( new double[][] { { 2.0 }, { 1.0 } }, m2 ) ) );
        final MatrixOutput q = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m2 );
        final MatrixOutput r = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m3 );
        final MatrixOutput u = MatrixOutput.of( new double[][] { { 1.0, 1.0 } }, m3 );
        final MatrixOutput v = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 }, { 1.0 } }, m3 );
        final MatrixOutput w = MatrixOutput.of( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, m3 );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertFalse( "Expected unequal outputs.", s.equals( q ) );
        assertFalse( "Expected unequal outputs.", q.equals( s ) );
        assertFalse( "Expected unequal outputs.", q.equals( r ) );
        assertFalse( "Expected unequal outputs.", r.equals( u ) );
        assertFalse( "Expected unequal outputs.", r.equals( v ) );
        assertFalse( "Expected unequal outputs.", r.equals( w ) );
        final MatrixOutput x =
                MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } },
                                            Arrays.asList( MetricDimension.ENSEMBLE_MEAN,
                                                           MetricDimension.ENSEMBLE_MEDIAN ),
                                            m1 );
        final MatrixOutput y =
                MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } },
                                            Arrays.asList( MetricDimension.ENSEMBLE_MEAN,
                                                           MetricDimension.ENSEMBLE_MEDIAN ),
                                            m1 );
        assertTrue( "Expected equal outputs.", x.equals( y ) );
        assertFalse( "Expected unequal outputs.", x.equals( w ) );
        assertFalse( "Expected unequal outputs.", x.equals( s ) );
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final Location l1 = MetadataFactory.getLocation( "A" );
        final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final MatrixOutput s = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput t = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final Location l1 = MetadataFactory.getLocation( "A" );
        final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = MetadataFactory.getLocation( "B" );
        final MetricOutputMetadata m2 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final MatrixOutput q = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput r = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m2 );
        assertFalse( "Metadata equal.", q.getMetadata().equals( r.getMetadata() ) );
        assertTrue( "Metadata unequal.", q.getMetadata().equals( m1 ) );
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final Location l1 = MetadataFactory.getLocation( "A" );
        final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );

        final MatrixOutput q = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput r = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput s =
                MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } },
                                            Arrays.asList( MetricDimension.ENSEMBLE_MEAN,
                                                           MetricDimension.ENSEMBLE_MEDIAN ),
                                            m1 );
        final MatrixOutput t =
                MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } },
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
     * Constructs a {@link MatrixOutput} and tests the {@link MatrixOutput#getComponentAtIndex(int)}.
     */

    @Test
    public void test5RowMajorIndex()
    {
        final Location l1 = MetadataFactory.getLocation( "A" );
        final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final MatrixOutput s =
                MatrixOutput.of( new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } },
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
     * Tests the exceptional cases associated with {@link MatrixOutput}.
     */

    @Test
    public void test6Exceptions()
    {
        final Location l1 = MetadataFactory.getLocation( "A" );
        final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( 10,
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        // Null raw data
        try
        {
            MatrixOutput.of( (MatrixOfDoubles) null, null, m1 );
            fail( "Expected an exception on attempting to construct a matrix output with null input." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Null metadata 
        try
        {
            MatrixOutput.of( new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } },
                                        null );
            fail( "Expected an exception on attempting to construct a matrix output with null metadata." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Wrong number of names
        try
        {
            MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } },
                                        Arrays.asList( MetricDimension.ENSEMBLE_MEAN ),
                                        m1 );
            fail( "Expected an exception on attempting to construct a matrix output with fewer named components than "
                  + "requrieDataFactory." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Attempting to access an incorrect index
        try
        {
            MatrixOutput test = MatrixOutput.of( new double[][] { { 1.0 }, { 1.0 } }, m1 );
            test.getComponentAtIndex( 3 );
            fail( "Expected an exception on attempting to access an incorrect index." );
        }
        catch ( IndexOutOfBoundsException e )
        {
        }
        // Attempting to access an incorrect index
        try
        {
            MatrixOutput test = MatrixOutput.of( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, m1 );
            test.getComponentAtIndex( 4 );
            fail( "Expected an exception on attempting to access an incorrect index." );
        }
        catch ( IndexOutOfBoundsException e )
        {
        }
    }


}

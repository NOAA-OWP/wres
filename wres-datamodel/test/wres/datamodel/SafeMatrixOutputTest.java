package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * Tests the {@link SafeMatrixOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeMatrixOutputTest
{

    /**
     * Constructs a {@link SafeMatrixOutput} and tests for equality with another {@link SafeMatrixOutput}.
     */

    @Test
    public void test1Equals()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A", "B", "C" ) );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata( 11,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A", "B", "C" ) );
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "B", "B", "C" ) );
        final MatrixOutput s = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput t = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertFalse( "Expected unequal outputs.", s.equals( null ) );
        assertFalse( "Expected unequal outputs.", s.equals( new Double( 1.0 ) ) );
        assertFalse( "Expected unequal outputs.",
                    s.equals( d.ofMatrixOutput( new double[][] { { 2.0 }, { 1.0 } }, m1 ) ) );
        assertFalse( "Expected unequal outputs.",
                    s.equals( d.ofMatrixOutput( new double[][] { { 2.0 }, { 1.0 } }, m2 ) ) );
        final MatrixOutput q = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m2 );
        final MatrixOutput r = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m3 );
        final MatrixOutput u = d.ofMatrixOutput( new double[][] { { 1.0, 1.0 } }, m3 );
        final MatrixOutput v = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 }, { 1.0 } }, m3 );
        final MatrixOutput w = d.ofMatrixOutput( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, m3 );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertFalse( "Expected unequal outputs.", s.equals( q ) );
        assertFalse( "Expected unequal outputs.", q.equals( s ) );
        assertFalse( "Expected unequal outputs.", q.equals( r ) );
        assertFalse( "Expected unequal outputs.", r.equals( u ) );
        assertFalse( "Expected unequal outputs.", r.equals( v ) );
        assertFalse( "Expected unequal outputs.", r.equals( w ) );
        final MatrixOutput x =
                d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } },
                                  Arrays.asList( MetricDimension.ENSEMBLE_MEAN, MetricDimension.ENSEMBLE_MEDIAN ),
                                  m1 );
        final MatrixOutput y =
                d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } },
                                  Arrays.asList( MetricDimension.ENSEMBLE_MEAN, MetricDimension.ENSEMBLE_MEDIAN ),
                                  m1 );       
        assertTrue( "Expected equal outputs.", x.equals( y ) );
        assertFalse( "Expected unequal outputs.", x.equals( w ) );
        assertFalse( "Expected unequal outputs.", x.equals( s ) );
    }

    /**
     * Constructs a {@link SafeMatrixOutput} and checks the {@link SafeMatrixOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("A", "B", "C"));
        final MatrixOutput s = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput t = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = d.getMetadataFactory().getOutputMetadata( 10,
                                                                                  metaFac.getDimension(),
                                                                                  metaFac.getDimension( "CMS" ),
                                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                                  MetricConstants.MAIN,
                                                                                  metaFac.getDatasetIdentifier( "A",
                                                                                                                "B",
                                                                                                                "C" ) );
        final MetricOutputMetadata m2 = d.getMetadataFactory().getOutputMetadata( 10,
                                                                                  metaFac.getDimension(),
                                                                                  metaFac.getDimension( "CMS" ),
                                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                                  MetricConstants.MAIN,
                                                                                  metaFac.getDatasetIdentifier( "B",
                                                                                                                "B",
                                                                                                                "C" ) );
        final MatrixOutput q = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput r = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m2 );
        assertFalse( "Metadata equal.", q.getMetadata().equals( r.getMetadata() ) );
        assertTrue( "Metadata unequal.", q.getMetadata().equals( m1 ) );        
    }

    /**
     * Constructs a {@link SafeMatrixOutput} and checks the {@link SafeMatrixOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A", "B", "C" ) );

        final DataFactory d = DefaultDataFactory.getInstance();
        final MatrixOutput q = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput r = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m1 );
        final MatrixOutput s =
                d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } },
                                  Arrays.asList( MetricDimension.ENSEMBLE_MEAN, MetricDimension.ENSEMBLE_MEDIAN ),
                                  m1 );
        final MatrixOutput t =
                d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } },
                                  Arrays.asList( MetricDimension.ENSEMBLE_MEAN, MetricDimension.ENSEMBLE_MEDIAN ),
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
     * Constructs a {@link SafeMatrixOutput} and tests the {@link SafeMatrixOutput#getComponentAtIndex(int)}.
     */

    @Test
    public void test5RowMajorIndex()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A", "B", "C" ) );
        final MatrixOutput s =
                d.ofMatrixOutput( new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } }, m1 );
        assertTrue( "Unexpected number of elements in the maxtrix.", s.size() == 9 );
        assertFalse( "Unexpected component names in the maxtrix.", s.hasComponentNames() );
        // Test the row-major indexing
        Iterator<Double>  iterator = s.iterator();
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
     * Tests the exceptional cases associated with {@link SafeMatrixOutput}.
     */
    
    @Test
    public void test6Exceptions()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("A", "B", "C"));
        // Null raw data
        try 
        {
             new SafeMatrixOutput(null, null, m1);
            fail( "Expected an exception on attempting to construct a matrix output with null input." );
        }
        catch( MetricOutputException e)
        {            
        }
        // Null metadata 
        try
        {
            d.ofMatrixOutput( new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } }, null );
            fail( "Expected an exception on attempting to construct a matrix output with null metadata." );
        }
        catch ( MetricOutputException e )
        {            
        }        
        // Wrong number of names
        try
        {
            d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, Arrays.asList( MetricDimension.ENSEMBLE_MEAN ), m1 );
            fail( "Expected an exception on attempting to construct a matrix output with fewer named components than "
                    + "requried." );
        }
        catch ( MetricOutputException e )
        {            
        }
        // Attempting to access an incorrect index
        try
        {
            MatrixOutput test = d.ofMatrixOutput( new double[][] { { 1.0 }, { 1.0 } }, m1 );
            test.getComponentAtIndex( 3 );
            fail( "Expected an exception on attempting to access an incorrect index." );
        }
        catch ( IndexOutOfBoundsException e )
        {            
        }
        // Attempting to access an incorrect index
        try
        {
            MatrixOutput test = d.ofMatrixOutput( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, m1 );
            test.getComponentAtIndex( 4 );
            fail( "Expected an exception on attempting to access an incorrect index." );
        }
        catch ( IndexOutOfBoundsException e )
        {            
        }
    }     
    
    
    

}

package wres.datamodel.outputs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.SafeMultiVectorOutput;

/**
 * Tests the {@link SafeMultiVectorOutput}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class SafeMultiVectorOutputTest
{

    /**
     * Constructs a {@link SafeMultiVectorOutput} and tests for equality with another {@link SafeMultiVectorOutput}.
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
        Map<MetricDimension, double[]> mva = new HashMap<>();
        Map<MetricDimension, double[]> mvb = new HashMap<>();
        Map<MetricDimension, double[]> mvc = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mvb.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mvb.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mvc.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4, 0.5 } );
        mvc.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4, 0.5 } );
        final MultiVectorOutput s = DataFactory.ofMultiVectorOutput( mva, m1 );
        final MultiVectorOutput t = DataFactory.ofMultiVectorOutput( mvb, m1 );
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( null ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( new Double( 1.0 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( DataFactory.ofMultiVectorOutput( mvc, m1 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( DataFactory.ofMultiVectorOutput( mvc, m2 ) ) );
        final MultiVectorOutput q = DataFactory.ofMultiVectorOutput( mva, m2 );
        final MultiVectorOutput r = DataFactory.ofMultiVectorOutput( mvb, m3 );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( s ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( r ) );
    }

    /**
     * Constructs a {@link SafeMultiVectorOutput} and checks the {@link SafeMultiVectorOutput#getMetadata()}.
     */

    @Test
    public void test2GetMetadata()
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
        Map<MetricDimension, double[]> mva = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        final MultiVectorOutput q = DataFactory.ofMultiVectorOutput( mva, m1 );
        final MultiVectorOutput r = DataFactory.ofMultiVectorOutput( mva, m2 );
        assertTrue( "Expected unequal dimensions.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link SafeMultiVectorOutput} and checks the {@link SafeMultiVectorOutput#hashCode()}.
     */

    @Test
    public void test3HashCode()
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
        final MetricOutputMetadata m2 = MetadataFactory.getOutputMetadata( 10,
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
        Map<MetricDimension, double[]> mva = new HashMap<>();
        Map<MetricDimension, double[]> mvb = new HashMap<>();
        Map<MetricDimension, double[]> mvc = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mvb.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mvb.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mvc.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4, 0.5 } );
        mvc.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4, 0.5 } );
        final MultiVectorOutput q = DataFactory.ofMultiVectorOutput( mva, m1 );
        final MultiVectorOutput r = DataFactory.ofMultiVectorOutput( mvb, m2 );
        final MultiVectorOutput s = DataFactory.ofMultiVectorOutput( mvc, m3 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.", q.hashCode() != s.hashCode() );
    }

    /**
     * Constructs a {@link SafeMultiVectorOutput} and checks the accessor methods for correct operation.
     */

    @Test
    public void test4Accessors()
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
        Map<MetricDimension, double[]> mva = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        final MultiVectorOutput s = DataFactory.ofMultiVectorOutput( mva, m1 );
        assertTrue( "Expected a " + MetricDimension.PROBABILITY_OF_DETECTION
                    + ".",
                    s.containsKey( MetricDimension.PROBABILITY_OF_DETECTION ) );
        assertTrue( "Expected a " + MetricDimension.PROBABILITY_OF_DETECTION
                    + ".",
                    s.get( MetricDimension.PROBABILITY_OF_DETECTION ) != null );
        assertTrue( "Expected a map of data.", !s.getData().isEmpty() );
    }

    /**
     * Attempts to construct a {@link SafeMultiVectorOutput} and checks for exceptions on invalid inputs.
     */

    @Test
    public void test5Exceptions()
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
        Map<MetricDimension, VectorOfDoubles> mva = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION,
                 DataFactory.vectorOf( new double[] { 0.1, 0.2, 0.3, 0.4 } ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION,
                 DataFactory.vectorOf( new double[] { 0.1, 0.2, 0.3, 0.4 } ) );
        try
        {
            new SafeMultiVectorOutput( mva, null );
            fail( "Expected an exception on null metadata." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeMultiVectorOutput( null, m1 );
            fail( "Expected an exception on null input data." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            mva.clear();
            new SafeMultiVectorOutput( mva, m1 );
            fail( "Expected an exception on empty inputs." );
        }
        catch ( MetricOutputException e )
        {
        }
    }


}

package wres.datamodel.outputs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.Location;
import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.SafeBoxPlotOutput;

/**
 * Tests the {@link SafeBoxPlotOutput}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class SafeBoxPlotOutputTest
{

    /**
     * Constructs a {@link SafeBoxPlotOutput} and tests for equality with another {@link SafeBoxPlotOutput}.
     */

    @Test
    public void test1Equals()
    {

        //Build datasets
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
        List<PairOfDoubleAndVectorOfDoubles> mva = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> mvb = new ArrayList<>();
        VectorOfDoubles pa = DataFactory.vectorOf( new double[] { 0.0, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3 } ) );
            mvb.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvc = new ArrayList<>();
        VectorOfDoubles pb = DataFactory.vectorOf( new double[] { 0.0, 0.25, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( DataFactory.pairOf( 1, new double[] { 2, 3, 4, 5 } ) );
        }

        final BoxPlotOutput q =
                new SafeBoxPlotOutput( mva, pa, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput r =
                new SafeBoxPlotOutput( mvb, pa, m3, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput s =
                new SafeBoxPlotOutput( mva, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput t =
                new SafeBoxPlotOutput( mvb, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput u =
                new SafeBoxPlotOutput( mvc, pb, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput v =
                new SafeBoxPlotOutput( mvc, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput w =
                new SafeBoxPlotOutput( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput x =
                new SafeBoxPlotOutput( mvd, pb, m2, MetricDimension.ENSEMBLE_MEAN, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput y =
                new SafeBoxPlotOutput( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_VALUE );
        final BoxPlotOutput z =
                new SafeBoxPlotOutput( mve, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        //Conduct comparisons
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( null ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( new Double( 1.0 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( u ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( v ) );
        assertTrue( "Expected non-equal outputs.", !v.equals( w ) );
        assertTrue( "Expected non-equal outputs.", !w.equals( x ) );
        assertTrue( "Expected non-equal outputs.", !w.equals( y ) );
        assertTrue( "Expected non-equal outputs.", !w.equals( z ) );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( s ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( r ) );
    }

    /**
     * Constructs a {@link SafeBoxPlotOutput} and tests for equal hashcodes with another {@link SafeBoxPlotOutput}.
     */

    @Test
    public void test2Hashcode()
    {

        //Build datasets
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
        List<PairOfDoubleAndVectorOfDoubles> mva = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> mvb = new ArrayList<>();
        VectorOfDoubles pa = DataFactory.vectorOf( new double[] { 0.0, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3 } ) );
            mvb.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvc = new ArrayList<>();
        VectorOfDoubles pb = DataFactory.vectorOf( new double[] { 0.0, 0.25, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( DataFactory.pairOf( 1, new double[] { 2, 3, 4, 5 } ) );
        }

        final BoxPlotOutput q =
                new SafeBoxPlotOutput( mva, pa, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput r =
                new SafeBoxPlotOutput( mvb, pa, m3, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput s =
                new SafeBoxPlotOutput( mva, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput t =
                new SafeBoxPlotOutput( mvb, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput u =
                new SafeBoxPlotOutput( mvc, pb, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput v =
                new SafeBoxPlotOutput( mvc, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput w =
                new SafeBoxPlotOutput( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput x =
                new SafeBoxPlotOutput( mvd, pb, m2, MetricDimension.ENSEMBLE_MEAN, MetricDimension.FORECAST_ERROR );
        final BoxPlotOutput y =
                new SafeBoxPlotOutput( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_VALUE );
        final BoxPlotOutput z =
                new SafeBoxPlotOutput( mve, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        //Conduct comparisons
        assertTrue( "Expected equal hashes.", s.hashCode() == t.hashCode() );
        assertTrue( "Expected non-equal hashes.", s.hashCode() != Objects.hash( (Object) null ) );
        assertTrue( "Expected non-equal hashes.", s.hashCode() != Double.hashCode( ( 1.0 ) ) );
        assertTrue( "Expected non-equal hashes.", s.hashCode() != u.hashCode() );
        assertTrue( "Expected non-equal hashes.", s.hashCode() != v.hashCode() );
        assertTrue( "Expected non-equal hashes.", v.hashCode() != w.hashCode() );
        assertTrue( "Expected non-equal hashes.", w.hashCode() != x.hashCode() );
        assertTrue( "Expected non-equal hashes.", w.hashCode() != y.hashCode() );
        assertTrue( "Expected non-equal hashes.", w.hashCode() != z.hashCode() );
        assertTrue( "Expected equal hashes.", q.hashCode() == q.hashCode() );
        assertTrue( "Expected non-equal hashes.", s.hashCode() != q.hashCode() );
        assertTrue( "Expected non-equal hashes.", q.hashCode() != s.hashCode() );
        assertTrue( "Expected non-equal hashes.", q.hashCode() != r.hashCode() );
    }

    /**
     * Constructs a {@link SafeBoxPlotOutput} and checks the {@link SafeBoxPlotOutput#getMetadata()}.
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
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        final SafeBoxPlotOutput q =
                new SafeBoxPlotOutput( values,
                                       DataFactory.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                       m1,
                                       MetricDimension.OBSERVED_VALUE,
                                       MetricDimension.FORECAST_ERROR );
        final SafeBoxPlotOutput r =
                new SafeBoxPlotOutput( values,
                                       DataFactory.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                       m2,
                                       MetricDimension.OBSERVED_VALUE,
                                       MetricDimension.FORECAST_ERROR );
        assertTrue( "Expected unequal dimensions.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link SafeBoxPlotOutput} and checks the accessor methods for correct operation.
     */

    @Test
    public void test3Accessors()
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
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        final SafeBoxPlotOutput q =
                new SafeBoxPlotOutput( values,
                                       DataFactory.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                       m1,
                                       MetricDimension.OBSERVED_VALUE,
                                       MetricDimension.FORECAST_ERROR );
        assertTrue( "Expected a list of data.", !q.getData().isEmpty() );
        assertTrue( "Expected an iterator with some elements to iterate.", q.iterator().hasNext() );
        assertTrue( "Unexpected probabilities associated with the box plot data.",
                    q.getProbabilities().equals( DataFactory.vectorOf( new double[] { 0.1, 0.5, 1.0 } ) ) );
        assertTrue( "Expected a domain axis dimension of " + MetricDimension.OBSERVED_VALUE
                    + ".",
                    q.getDomainAxisDimension().equals( MetricDimension.OBSERVED_VALUE ) );
        assertTrue( "Expected a range axis dimension of " + MetricDimension.FORECAST_ERROR
                    + ".",
                    q.getRangeAxisDimension().equals( MetricDimension.FORECAST_ERROR ) );
    }

    /**
     * Attempts to construct a {@link SafeBoxPlotOutput} and checks for exceptions on invalid inputs.
     */

    @Test
    public void test4Exceptions()
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
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( DataFactory.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        try
        {
            new SafeBoxPlotOutput( null,
                                   DataFactory.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on null input data." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   DataFactory.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                   null,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on null metadata." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   null,
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on a null vector of probabilities." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   DataFactory.vectorOf( new double[] {} ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on an empty vector of probabilities." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   DataFactory.vectorOf( new double[] { 5.0, 10.0, 15.0 } ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on invalid probabilities." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   DataFactory.vectorOf( new double[] { 5.0, 10.0 } ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on fewer probabilities than whiskers." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            final List<PairOfDoubleAndVectorOfDoubles> uneven = new ArrayList<>();
            uneven.add( DataFactory.pairOf( 1.0, new double[] { 1, 2, 3 } ) );
            uneven.add( DataFactory.pairOf( 1.0, new double[] { 1, 2, 3, 4 } ) );
            new SafeBoxPlotOutput( uneven,
                                   DataFactory.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on boxes with varying numbers of whiskers." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            final List<PairOfDoubleAndVectorOfDoubles> uneven = new ArrayList<>();
            uneven.add( DataFactory.pairOf( 1.0, new double[] { 1, 2, 3 } ) );
            uneven.add( DataFactory.pairOf( 1.0, new double[] {} ) );
            new SafeBoxPlotOutput( uneven,
                                   DataFactory.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on boxes with missing whiskers." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   DataFactory.vectorOf( new double[] { 0.0, -0.5, 1.0 } ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on boxes with invalid probabilities." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   DataFactory.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
                                   m1,
                                   null,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on a null domain axis dimension." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            new SafeBoxPlotOutput( values,
                                   DataFactory.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
                                   m1,
                                   MetricDimension.OBSERVED_VALUE,
                                   null );
            fail( "Expected an exception on a null range axis dimension." );
        }
        catch ( MetricOutputException e )
        {
        }
    }


}

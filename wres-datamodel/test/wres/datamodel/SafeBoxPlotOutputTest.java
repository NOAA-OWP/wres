package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * Tests the {@link SafeBoxPlotOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeBoxPlotOutputTest
{

    /**
     * Constructs a {@link SafeBoxPlotOutput} and tests for equality with another {@link SafeBoxPlotOutput}.
     */

    @Test
    public void test1Equals()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        //Build datasets
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
        List<PairOfDoubleAndVectorOfDoubles> mva = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> mvb = new ArrayList<>();
        VectorOfDoubles pa = d.vectorOf( new double[] { 0.0, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( d.pairOf( 1, new double[] { 1, 2, 3 } ) );
            mvb.add( d.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvc = new ArrayList<>();
        VectorOfDoubles pb = d.vectorOf( new double[] { 0.0, 0.25, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( d.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( d.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( d.pairOf( 1, new double[] { 2, 3, 4, 5 } ) );
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
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        //Build datasets
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
        List<PairOfDoubleAndVectorOfDoubles> mva = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> mvb = new ArrayList<>();
        VectorOfDoubles pa = d.vectorOf( new double[] { 0.0, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( d.pairOf( 1, new double[] { 1, 2, 3 } ) );
            mvb.add( d.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvc = new ArrayList<>();
        VectorOfDoubles pb = d.vectorOf( new double[] { 0.0, 0.25, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( d.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( d.pairOf( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<PairOfDoubleAndVectorOfDoubles> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( d.pairOf( 1, new double[] { 2, 3, 4, 5 } ) );
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
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A", "B", "C" ) );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "B", "B", "C" ) );
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( d.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        final SafeBoxPlotOutput q =
                new SafeBoxPlotOutput( values,
                                       d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                       m1,
                                       MetricDimension.OBSERVED_VALUE,
                                       MetricDimension.FORECAST_ERROR );
        final SafeBoxPlotOutput r =
                new SafeBoxPlotOutput( values,
                                       d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
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
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A", "B", "C" ) );
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( d.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        final SafeBoxPlotOutput q =
                new SafeBoxPlotOutput( values,
                                       d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                       m1,
                                       MetricDimension.OBSERVED_VALUE,
                                       MetricDimension.FORECAST_ERROR );
        assertTrue( "Expected a list of data.", !q.getData().isEmpty() );
        assertTrue( "Expected an iterator with some elements to iterate.", q.iterator().hasNext() );
        assertTrue( "Unexpected probabilities associated with the box plot data.",
                    q.getProbabilities().equals( d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ) ) );
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
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A", "B", "C" ) );
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( d.pairOf( 1, new double[] { 1, 2, 3 } ) );
        }
        try
        {
            new SafeBoxPlotOutput( null,
                                   d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
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
                                   d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
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
                                   d.vectorOf( new double[] {} ),
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
                                   d.vectorOf( new double[] { 5.0, 10.0, 15.0 } ),
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
                                   d.vectorOf( new double[] { 5.0, 10.0 } ),
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
            uneven.add( d.pairOf( 1.0, new double[] { 1, 2, 3 } ) );
            uneven.add( d.pairOf( 1.0, new double[] { 1, 2, 3, 4 } ) );
            new SafeBoxPlotOutput( uneven,
                                   d.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
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
            uneven.add( d.pairOf( 1.0, new double[] { 1, 2, 3 } ) );
            uneven.add( d.pairOf( 1.0, new double[] {} ) );
            new SafeBoxPlotOutput( uneven,
                                   d.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
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
                                   d.vectorOf( new double[] { 0.0, -0.5, 1.0 } ),
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
                                   d.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
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
                                   d.vectorOf( new double[] { 0.0, 0.5, 1.0 } ),
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

package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.MetricConstants.MetricDimension;

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
     * Constructs a {@link SafeBoxPlotOutput} and checks the {@link SafeBoxPlotOutput#getMetadata()}.
     */

    @Test
    public void test1GetMetadata()
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
                                       m1,
                                       d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                       MetricDimension.OBSERVED_VALUE,
                                       MetricDimension.FORECAST_ERROR );
        final SafeBoxPlotOutput r =
                new SafeBoxPlotOutput( values,
                                       m2,
                                       d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                       MetricDimension.OBSERVED_VALUE,
                                       MetricDimension.FORECAST_ERROR );
        assertTrue( "Expected unequal dimensions.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link SafeBoxPlotOutput} and checks the accessor methods for correct operation.
     */

    @Test
    public void test2Accessors()
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
                                       m1,
                                       d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
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
    public void test3Exceptions()
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
                                   m1,
                                   d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on null input data." );
        }
        catch ( MetricOutputException e )
        {
        }
        try
        {
            List<PairOfDoubleAndVectorOfDoubles> emptyList = new ArrayList<>();
            new SafeBoxPlotOutput( emptyList,
                                   m1,
                                   d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
                                   MetricDimension.OBSERVED_VALUE,
                                   MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on an empty list of input data." );
        }
        catch ( MetricOutputException e )
        {
        }        
        try
        {
            new SafeBoxPlotOutput( values,
                                   null,
                                   d.vectorOf( new double[] { 0.1, 0.5, 1.0 } ),
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
                                   m1,
                                   null,
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
                                   m1,
                                   d.vectorOf( new double[] {} ),
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
                                   m1,
                                   d.vectorOf( new double[] {5.0, 10.0, 15.0} ),
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
                                   m1,
                                   d.vectorOf( new double[] {5.0, 10.0} ),
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
            uneven.add( d.pairOf( 1.0, new double[] {1,2,3} ) );
            uneven.add( d.pairOf( 1.0, new double[] {1,2,3,4} ) );
            new SafeBoxPlotOutput( uneven,
                                   m1,
                                   d.vectorOf( new double[] {0.0, 0.5, 1.0} ),
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
            uneven.add( d.pairOf( 1.0, new double[] {1,2,3} ) );
            uneven.add( d.pairOf( 1.0, new double[] {} ) );
            new SafeBoxPlotOutput( uneven,
                                   m1,
                                   d.vectorOf( new double[] {0.0, 0.5, 1.0} ),
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
                                   m1,
                                   d.vectorOf( new double[] {0.0, -0.5, 1.0} ),
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
                                   m1,
                                   d.vectorOf( new double[] {0.0,0.5,1.0} ),
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
                                   m1,
                                   d.vectorOf( new double[] {0.0,0.5,1.0} ),
                                   MetricDimension.OBSERVED_VALUE,
                                   null );
            fail( "Expected an exception on a null range axis dimension." );
        }
        catch ( MetricOutputException e )
        {
        } 
    }


}

package wres.datamodel.statistics;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.pairs.EnsemblePair;

/**
 * Tests the {@link BoxPlotStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class BoxPlotStatisticTest
{

    /**
     * Constructs a {@link BoxPlotStatistic} and tests for equality with another {@link BoxPlotStatistic}.
     */

    @Test
    public void test1Equals()
    {

        //Build datasets
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
                                                           12,
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
        List<EnsemblePair> mva = new ArrayList<>();
        List<EnsemblePair> mvb = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( new double[] { 0.0, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
            mvb.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        }
        List<EnsemblePair> mvc = new ArrayList<>();
        VectorOfDoubles pb = VectorOfDoubles.of( new double[] { 0.0, 0.25, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( EnsemblePair.of( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<EnsemblePair> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( EnsemblePair.of( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<EnsemblePair> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( EnsemblePair.of( 1, new double[] { 2, 3, 4, 5 } ) );
        }

        final BoxPlotStatistic q =
                BoxPlotStatistic.of( mva, pa, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic r =
                BoxPlotStatistic.of( mvb, pa, m3, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic s =
                BoxPlotStatistic.of( mva, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic t =
                BoxPlotStatistic.of( mvb, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic u =
                BoxPlotStatistic.of( mvc, pb, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic v =
                BoxPlotStatistic.of( mvc, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic w =
                BoxPlotStatistic.of( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic x =
                BoxPlotStatistic.of( mvd, pb, m2, MetricDimension.ENSEMBLE_MEAN, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic y =
                BoxPlotStatistic.of( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_VALUE );
        final BoxPlotStatistic z =
                BoxPlotStatistic.of( mve, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
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
     * Constructs a {@link BoxPlotStatistic} and tests for equal hashcodes with another {@link BoxPlotStatistic}.
     */

    @Test
    public void test2Hashcode()
    {

        //Build datasets
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
        List<EnsemblePair> mva = new ArrayList<>();
        List<EnsemblePair> mvb = new ArrayList<>();
        VectorOfDoubles pa = VectorOfDoubles.of( new double[] { 0.0, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mva.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
            mvb.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        }
        List<EnsemblePair> mvc = new ArrayList<>();
        VectorOfDoubles pb = VectorOfDoubles.of( new double[] { 0.0, 0.25, 0.5, 1.0 } );
        for ( int i = 0; i < 10; i++ )
        {
            mvc.add( EnsemblePair.of( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<EnsemblePair> mvd = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mvd.add( EnsemblePair.of( 1, new double[] { 1, 2, 3, 4 } ) );
        }
        List<EnsemblePair> mve = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            mve.add( EnsemblePair.of( 1, new double[] { 2, 3, 4, 5 } ) );
        }

        final BoxPlotStatistic q =
                BoxPlotStatistic.of( mva, pa, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic r =
                BoxPlotStatistic.of( mvb, pa, m3, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic s =
                BoxPlotStatistic.of( mva, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic t =
                BoxPlotStatistic.of( mvb, pa, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic u =
                BoxPlotStatistic.of( mvc, pb, m1, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic v =
                BoxPlotStatistic.of( mvc, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic w =
                BoxPlotStatistic.of( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic x =
                BoxPlotStatistic.of( mvd, pb, m2, MetricDimension.ENSEMBLE_MEAN, MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic y =
                BoxPlotStatistic.of( mvd, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_VALUE );
        final BoxPlotStatistic z =
                BoxPlotStatistic.of( mve, pb, m2, MetricDimension.OBSERVED_VALUE, MetricDimension.FORECAST_ERROR );
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
     * Constructs a {@link BoxPlotStatistic} and checks the {@link BoxPlotStatistic#getMetadata()}.
     */

    @Test
    public void test2GetMetadata()
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
        final List<EnsemblePair> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        }
        final BoxPlotStatistic q =
                BoxPlotStatistic.of( values,
                                     VectorOfDoubles.of( new double[] { 0.1, 0.5, 1.0 } ),
                                     m1,
                                     MetricDimension.OBSERVED_VALUE,
                                     MetricDimension.FORECAST_ERROR );
        final BoxPlotStatistic r =
                BoxPlotStatistic.of( values,
                                     VectorOfDoubles.of( new double[] { 0.1, 0.5, 1.0 } ),
                                     m2,
                                     MetricDimension.OBSERVED_VALUE,
                                     MetricDimension.FORECAST_ERROR );
        assertTrue( "Expected unequal dimensions.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link BoxPlotStatistic} and checks the accessor methods for correct operation.
     */

    @Test
    public void test3Accessors()
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
        ;
        final List<EnsemblePair> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        }
        final BoxPlotStatistic q =
                BoxPlotStatistic.of( values,
                                     VectorOfDoubles.of( new double[] { 0.1, 0.5, 1.0 } ),
                                     m1,
                                     MetricDimension.OBSERVED_VALUE,
                                     MetricDimension.FORECAST_ERROR );
        assertTrue( "Expected a list of data.", !q.getData().isEmpty() );
        assertTrue( "Expected an iterator with some elements to iterate.", q.iterator().hasNext() );
        assertTrue( "Unexpected probabilities associated with the box plot data.",
                    q.getProbabilities().equals( VectorOfDoubles.of( new double[] { 0.1, 0.5, 1.0 } ) ) );
        assertTrue( "Expected a domain axis dimension of " + MetricDimension.OBSERVED_VALUE
                    + ".",
                    q.getDomainAxisDimension().equals( MetricDimension.OBSERVED_VALUE ) );
        assertTrue( "Expected a range axis dimension of " + MetricDimension.FORECAST_ERROR
                    + ".",
                    q.getRangeAxisDimension().equals( MetricDimension.FORECAST_ERROR ) );
    }

    /**
     * Attempts to construct a {@link BoxPlotStatistic} and checks for exceptions on invalid inputs.
     */

    @Test
    public void test4Exceptions()
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
        final List<EnsemblePair> values = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        }
        try
        {
            BoxPlotStatistic.of( null,
                                 VectorOfDoubles.of( new double[] { 0.1, 0.5, 1.0 } ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on null input data." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 VectorOfDoubles.of( new double[] { 0.1, 0.5, 1.0 } ),
                                 null,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on null metadata." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 null,
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on a null vector of probabilities." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 VectorOfDoubles.of( new double[] {} ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on an empty vector of probabilities." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 VectorOfDoubles.of( new double[] { 5.0, 10.0, 15.0 } ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on invalid probabilities." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 VectorOfDoubles.of( new double[] { 5.0, 10.0 } ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on fewer probabilities than whiskers." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            final List<EnsemblePair> uneven = new ArrayList<>();
            uneven.add( EnsemblePair.of( 1.0, new double[] { 1, 2, 3 } ) );
            uneven.add( EnsemblePair.of( 1.0, new double[] { 1, 2, 3, 4 } ) );
            BoxPlotStatistic.of( uneven,
                                 VectorOfDoubles.of( new double[] { 0.0, 0.5, 1.0 } ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on boxes with varying numbers of whiskers." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            final List<EnsemblePair> uneven = new ArrayList<>();
            uneven.add( EnsemblePair.of( 1.0, new double[] { 1, 2, 3 } ) );
            uneven.add( EnsemblePair.of( 1.0, new double[] {} ) );
            BoxPlotStatistic.of( uneven,
                                 VectorOfDoubles.of( new double[] { 0.0, 0.5, 1.0 } ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on boxes with missing whiskers." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 VectorOfDoubles.of( new double[] { 0.0, -0.5, 1.0 } ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on boxes with invalid probabilities." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 VectorOfDoubles.of( new double[] { 0.0, 0.5, 1.0 } ),
                                 m1,
                                 null,
                                 MetricDimension.FORECAST_ERROR );
            fail( "Expected an exception on a null domain axis dimension." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            BoxPlotStatistic.of( values,
                                 VectorOfDoubles.of( new double[] { 0.0, 0.5, 1.0 } ),
                                 m1,
                                 MetricDimension.OBSERVED_VALUE,
                                 null );
            fail( "Expected an exception on a null range axis dimension." );
        }
        catch ( StatisticException e )
        {
        }
    }


}

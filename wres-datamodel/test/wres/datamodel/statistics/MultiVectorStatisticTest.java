package wres.datamodel.statistics;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.statistics.StatisticException;
import wres.datamodel.statistics.MultiVectorStatistic;

/**
 * Tests the {@link MultiVectorStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class MultiVectorStatisticTest
{

    /**
     * Constructs a {@link MultiVectorStatistic} and tests for equality with another {@link MultiVectorStatistic}.
     */

    @Test
    public void test1Equals()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( 11,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l3,
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
        final MultiVectorStatistic s = MultiVectorStatistic.ofMultiVectorOutput( mva, m1 );
        final MultiVectorStatistic t = MultiVectorStatistic.ofMultiVectorOutput( mvb, m1 );
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( null ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( new Double( 1.0 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( MultiVectorStatistic.ofMultiVectorOutput( mvc, m1 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( MultiVectorStatistic.ofMultiVectorOutput( mvc, m2 ) ) );
        final MultiVectorStatistic q = MultiVectorStatistic.ofMultiVectorOutput( mva, m2 );
        final MultiVectorStatistic r = MultiVectorStatistic.ofMultiVectorOutput( mvb, m3 );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( s ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( r ) );
    }

    /**
     * Constructs a {@link MultiVectorStatistic} and checks the {@link MultiVectorStatistic#getMetadata()}.
     */

    @Test
    public void test2GetMetadata()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = Location.of( "B" );
        final StatisticMetadata m2 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        Map<MetricDimension, double[]> mva = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        final MultiVectorStatistic q = MultiVectorStatistic.ofMultiVectorOutput( mva, m1 );
        final MultiVectorStatistic r = MultiVectorStatistic.ofMultiVectorOutput( mva, m2 );
        assertTrue( "Expected unequal dimensions.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link MultiVectorStatistic} and checks the {@link MultiVectorStatistic#hashCode()}.
     */

    @Test
    public void test3HashCode()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l3,
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
        final MultiVectorStatistic q = MultiVectorStatistic.ofMultiVectorOutput( mva, m1 );
        final MultiVectorStatistic r = MultiVectorStatistic.ofMultiVectorOutput( mvb, m2 );
        final MultiVectorStatistic s = MultiVectorStatistic.ofMultiVectorOutput( mvc, m3 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.", q.hashCode() != s.hashCode() );
    }

    /**
     * Constructs a {@link MultiVectorStatistic} and checks the accessor methods for correct operation.
     */

    @Test
    public void test4Accessors()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        Map<MetricDimension, double[]> mva = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, new double[] { 0.1, 0.2, 0.3, 0.4 } );
        final MultiVectorStatistic s = MultiVectorStatistic.ofMultiVectorOutput( mva, m1 );
        assertTrue( "Expected a " + MetricDimension.PROBABILITY_OF_DETECTION
                    + ".",
                    s.containsKey( MetricDimension.PROBABILITY_OF_DETECTION ) );
        assertTrue( "Expected a " + MetricDimension.PROBABILITY_OF_DETECTION
                    + ".",
                    s.get( MetricDimension.PROBABILITY_OF_DETECTION ) != null );
        assertTrue( "Expected a map of data.", !s.getData().isEmpty() );
    }

    /**
     * Attempts to construct a {@link MultiVectorStatistic} and checks for exceptions on invalid inputs.
     */

    @Test
    public void test5Exceptions()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        Map<MetricDimension, VectorOfDoubles> mva = new HashMap<>();
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION,
                 VectorOfDoubles.of( new double[] { 0.1, 0.2, 0.3, 0.4 } ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION,
                 VectorOfDoubles.of( new double[] { 0.1, 0.2, 0.3, 0.4 } ) );
        try
        {
            MultiVectorStatistic.of( mva, null );
            fail( "Expected an exception on null metadata." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            MultiVectorStatistic.of( null, m1 );
            fail( "Expected an exception on null input data." );
        }
        catch ( StatisticException e )
        {
        }
        try
        {
            mva.clear();
            MultiVectorStatistic.of( mva, m1 );
            fail( "Expected an exception on empty inputs." );
        }
        catch ( StatisticException e )
        {
        }
    }


}

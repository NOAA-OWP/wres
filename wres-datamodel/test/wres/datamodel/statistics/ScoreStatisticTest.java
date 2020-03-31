package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Tests the {@link ScoreStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class ScoreStatisticTest
{

    /**
     * Constructs a {@link DoubleScoreStatistic} and tests for equality with another {@link DoubleScoreStatistic}.
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
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           11,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final ScoreStatistic<Double, DoubleScoreStatistic> s = DoubleScoreStatistic.of( 1.0, m1 );
        final ScoreStatistic<Double, DoubleScoreStatistic> t = DoubleScoreStatistic.of( 1.0, m1 );
        assertTrue( s.equals( t ) );
        assertNotEquals( null, s );
        assertNotEquals( Double.valueOf( 1.0 ), s );
        assertTrue( !s.equals( DoubleScoreStatistic.of( 2.0, m1 ) ) );
        assertTrue( !s.equals( DoubleScoreStatistic.of( 1.0, m2 ) ) );
        final ScoreStatistic<Double, DoubleScoreStatistic> q = DoubleScoreStatistic.of( 1.0, m2 );
        final ScoreStatistic<Double, DoubleScoreStatistic> r = DoubleScoreStatistic.of( 1.0, m3 );
        assertTrue( !s.equals( q ) );
        assertEquals( q, q );
        assertTrue( !q.equals( s ) );
        assertTrue( !q.equals( r ) );
    }

    /**
     * Constructs a {@link DoubleScoreStatistic} and checks the {@link DoubleScoreStatistic#toString()} representation.
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
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final ScoreStatistic<Double, DoubleScoreStatistic> s = DoubleScoreStatistic.of( 1.0, m1 );
        final ScoreStatistic<Double, DoubleScoreStatistic> t = DoubleScoreStatistic.of( 1.0, m1 );
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link DoubleScoreStatistic} and checks the {@link DoubleScoreStatistic#getMetadata()}.
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
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "B" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final ScoreStatistic<Double, DoubleScoreStatistic> q = DoubleScoreStatistic.of( 1.0, m1 );
        final ScoreStatistic<Double, DoubleScoreStatistic> r = DoubleScoreStatistic.of( 1.0, m2 );
        assertTrue( "Unequal metadata.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link DoubleScoreStatistic} and checks the {@link DoubleScoreStatistic#hashCode()}.
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
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final ScoreStatistic<Double, DoubleScoreStatistic> q = DoubleScoreStatistic.of( 1.0, m1 );
        final ScoreStatistic<Double, DoubleScoreStatistic> r = DoubleScoreStatistic.of( 1.0, m2 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.",
                    q.hashCode() != DoubleScoreStatistic.of( 1.0, m3 ).hashCode() );
    }

}

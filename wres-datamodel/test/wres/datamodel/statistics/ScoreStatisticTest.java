package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants;
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
        final FeatureKey l1 = FeatureKey.of( "A" );
        final FeatureTuple locationOne = new FeatureTuple( l1, l1, l1 );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationOne,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final FeatureKey l2 = FeatureKey.of( "B" );
        final FeatureTuple locationTwo = new FeatureTuple( l2, l2, l2 );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationTwo,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           11,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final FeatureKey l3 = FeatureKey.of( "B" );
        final FeatureTuple locationThree = new FeatureTuple( l3, l3, l3 );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationThree,
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
        final FeatureKey l1 = FeatureKey.of( "A" );
        final FeatureTuple locationOne = new FeatureTuple( l1, l1, l1 );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationOne,
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
        final FeatureKey l1 = FeatureKey.of( "A" );
        final FeatureTuple locationOne = new FeatureTuple( l1, l1, l1 );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationOne,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final FeatureKey l2 = FeatureKey.of( "B" );
        final FeatureTuple locationTwo = new FeatureTuple( l2, l2, l2 );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationTwo,
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
        final FeatureKey l1 = FeatureKey.of( "A" );
        final FeatureTuple locationOne = new FeatureTuple( l1, l1, l1 );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationOne,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final FeatureKey l2 = FeatureKey.of( "A" );
        final FeatureTuple locationTwo = new FeatureTuple( l2, l2, l2 );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationTwo,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_ERROR,
                                                           MetricConstants.MAIN );
        final FeatureKey l3 = FeatureKey.of( "B" );
        final FeatureTuple locationThree = new FeatureTuple( l3, l3, l3 );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( locationThree,
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

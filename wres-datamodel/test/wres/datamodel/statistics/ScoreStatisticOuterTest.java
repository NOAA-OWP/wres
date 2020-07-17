package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ScoreStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class ScoreStatisticOuterTest
{

    /**
     * Instant to use when testing.
     */

    private final DoubleScoreStatistic one =
            DoubleScoreStatistic.newBuilder()
                                .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                             .setValue( 1.0 )
                                                                             .setName( ComponentName.MAIN ) )
                                .build();

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and tests for equality with another {@link DoubleScoreStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        FeatureKey l1 = FeatureKey.of( "A" );
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ),
                                                                     "B",
                                                                     "C" ) );
        FeatureKey l2 = FeatureKey.of( "A" );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ),
                                                                     "B",
                                                                     "C" ) );
        FeatureKey l3 = FeatureKey.of( "B" );
        SampleMetadata m3 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l3, l3, l3 ),
                                                                     "B",
                                                                     "C" ) );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> s =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> t =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        assertTrue( s.equals( t ) );
        assertNotEquals( null, s );
        assertNotEquals( Double.valueOf( 1.0 ), s );

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        assertNotEquals( DoubleScoreStatisticOuter.of( two, m1 ), s );
        assertNotEquals( DoubleScoreStatisticOuter.of( this.one, m3 ), s );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, m2 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, m3 );
        assertEquals( q, q );
        assertNotEquals( q, r );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#toString()} representation.
     */

    @Test
    public void testToString()
    {
        FeatureKey l1 = FeatureKey.of( "A" );
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of(new FeatureTuple( l1, l1, l1 ),
                                                                     "B",
                                                                     "C" ) );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> s =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> t =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        assertEquals( "Expected equal string representations.", s.toString(), t.toString() );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        FeatureKey l1 = FeatureKey.of( "A" );
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ),
                                                                     "B",
                                                                     "C" ) );
        FeatureKey l2 = FeatureKey.of( "B" );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ),
                                                                     "B",
                                                                     "C" ) );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, m2 );
        assertNotEquals( "Equal metadata.", q.getMetadata(), r.getMetadata() );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        FeatureKey l1 = FeatureKey.of( "A" );
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ),
                                                                     "B",
                                                                     "C" ) );
        FeatureKey l2 = FeatureKey.of( "A" );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ),
                                                                     "B",
                                                                     "C" ) );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, m2 );
        assertEquals( "Expected equal hash codes.", q.hashCode(), r.hashCode() );
    }

}

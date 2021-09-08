package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
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

    private DoubleScoreStatistic one;

    /**
     * Default metadata for testing.
     */

    private PoolMetadata metadata;

    @Before
    public void runBeforeEachTest()
    {
        FeatureKey feature = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();


        Pool pool = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( feature, feature, feature ) ),
                                          null,
                                          null,
                                          null,
                                          false );
        this.metadata = PoolMetadata.of( evaluation, pool );

        this.one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN ) ) )
                                    .build();
    }


    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and tests for equality with another {@link DoubleScoreStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        FeatureKey l1 = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();


        Pool pool = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( l1, l1, l1 ) ),
                                          null,
                                          null,
                                          null,
                                          false );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );

        PoolMetadata m2 = PoolMetadata.of( evaluation, pool );

        FeatureKey l3 = FeatureKey.of( "B" );

        Pool poolTwo = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( l3, l3, l3 ) ),
                                             null,
                                             null,
                                             null,
                                             false );

        PoolMetadata m3 = PoolMetadata.of( evaluation, poolTwo );

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
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN ) ) )
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
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> s =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> t =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        assertEquals( "Expected equal string representations.", s.toString(), t.toString() );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        assertEquals( q.getMetadata(), r.getMetadata() );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        assertEquals( "Expected equal hash codes.", q.hashCode(), r.hashCode() );
    }

}

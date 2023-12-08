package wres.datamodel.statistics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.SummaryStatistic;

/**
 * Tests the {@link ScoreStatistic}.
 *
 * @author James Brown
 */
class ScoreStatisticOuterTest
{
    /** Instant to use when testing. */
    private DoubleScoreStatistic one;

    /** Default metadata for testing. */
    private PoolMetadata metadata;

    /** Feature group. */
    private FeatureGroup featureGroup;

    /** Summary statistic. */
    private SummaryStatistic summaryStatistic;

    @BeforeEach
    public void runBeforeEachTest()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Geometry geometry = wres.statistics.MessageFactory.getGeometry( "A" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        this.featureGroup = FeatureGroup.of( geoGroup );

        Pool pool = MessageFactory.getPool( this.featureGroup,
                                            null,
                                            null,
                                            null,
                                            false,
                                            1 );
        this.metadata = PoolMetadata.of( evaluation, pool );

        this.one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        this.summaryStatistic = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                .setDimension( SummaryStatistic.StatisticDimension.RESAMPLED )
                                                .setProbability( 0.25 )
                                                .build();
    }

    @Test
    void testEquals()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();


        Pool pool = MessageFactory.getPool( this.featureGroup,
                                            null,
                                            null,
                                            null,
                                            false,
                                            1 );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );

        PoolMetadata m2 = PoolMetadata.of( evaluation, pool );

        Geometry geometry = wres.statistics.MessageFactory.getGeometry( "B" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup anotherFeatureGroup = FeatureGroup.of( geoGroup );

        Pool poolTwo = MessageFactory.getPool( anotherFeatureGroup,
                                               null,
                                               null,
                                               null,
                                               false,
                                               1 );

        PoolMetadata m3 = PoolMetadata.of( evaluation, poolTwo );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> s =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> t =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        assertEquals( s, t );
        assertNotEquals( null, s );
        assertNotEquals( 1.0, s );

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        assertNotEquals( DoubleScoreStatisticOuter.of( two, m1 ), s );
        assertNotEquals( DoubleScoreStatisticOuter.of( this.one, m3 ), s );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, m2 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, m3 );
        assertEquals( q, q );
        assertNotEquals( q, r );

        DoubleScoreStatisticOuter u = DoubleScoreStatisticOuter.of( two, m1, this.summaryStatistic );
        DoubleScoreStatisticOuter v = DoubleScoreStatisticOuter.of( this.one, m1, null );
        assertEquals( u, u );
        assertEquals( s, v );
        assertNotEquals( u, v );
    }

    @Test
    void testToString()
    {
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> s =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> t =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        assertEquals( s.toString(), t.toString() );
    }

    @Test
    void testGetMetadata()
    {
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, this.metadata );
        assertEquals( q.getPoolMetadata(), r.getPoolMetadata() );
    }

    @Test
    void testHashCode()
    {
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, this.metadata, this.summaryStatistic );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, this.metadata, this.summaryStatistic );
        assertEquals( q.hashCode(), r.hashCode() );
    }

    @Test
    void testGetSampleQuantile()
    {
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, this.metadata, this.summaryStatistic );
        assertEquals( 0.25, q.getSummaryStatistic()
                             .getProbability() );
    }
}

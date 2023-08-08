package wres.datamodel.statistics;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;

/**
 * Tests the {@link DurationDiagramStatisticOuter}.
 *
 * @author James Brown
 */
class DurationDiagramStatisticOuterTest
{
    /** Default instance for testing. */
    private DurationDiagramStatistic defaultInstance;

    /** Metadata for testing. */
    private PoolMetadata metadata;

    /** Feature group. */
    private FeatureGroup featureGroup;

    @BeforeEach
    void runBeforeEachTest()
    {
        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .build();

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( 123456 )
                                                                                  .setNanos( 789 ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 13 ) )
                                                               .build();

        this.defaultInstance = DurationDiagramStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( one )
                                                       .build();

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

        DurationDiagramStatisticOuter one = DurationDiagramStatisticOuter.of( this.defaultInstance, m2 );

        assertEquals( one, one );

        assertNotEquals( null, one );

        assertNotEquals( 1.0, one );

        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .build();

        PairOfInstantAndDuration first =
                PairOfInstantAndDuration.newBuilder()
                                        .setTime( Timestamp.newBuilder()
                                                           .setSeconds( 123456 )
                                                           .setNanos( 689 ) )
                                        .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                  .setSeconds( 13 ) )
                                        .build();

        DurationDiagramStatistic next = DurationDiagramStatistic.newBuilder()
                                                                .setMetric( metric )
                                                                .addStatistics( first )
                                                                .build();

        DurationDiagramStatisticOuter another = DurationDiagramStatisticOuter.of( next, this.metadata );

        assertNotEquals( one, another );
        assertNotEquals( one, DurationDiagramStatisticOuter.of( this.defaultInstance, m3 ) );


        PairOfInstantAndDuration second =
                PairOfInstantAndDuration.newBuilder()
                                        .setTime( Timestamp.newBuilder()
                                                           .setSeconds( 123457 )
                                                           .setNanos( 689 ) )
                                        .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                  .setSeconds( 13 ) )
                                        .build();

        DurationDiagramStatistic anotherNext = DurationDiagramStatistic.newBuilder()
                                                                       .setMetric( metric )
                                                                       .addStatistics( second )
                                                                       .build();

        DurationDiagramStatisticOuter yetAnother = DurationDiagramStatisticOuter.of( anotherNext, m2 );
        DurationDiagramStatisticOuter oneMore = DurationDiagramStatisticOuter.of( anotherNext, m3 );

        assertNotEquals( another, yetAnother );
        assertNotEquals( yetAnother, another );
        assertEquals( yetAnother, yetAnother );
        assertNotEquals( yetAnother, oneMore );

        DurationDiagramStatisticOuter u = DurationDiagramStatisticOuter.of( anotherNext, m2, 0.25 );
        DurationDiagramStatisticOuter v = DurationDiagramStatisticOuter.of( next, this.metadata, null );
        assertEquals( u, u );
        assertEquals( another, v );
        assertNotEquals( u, v );
    }

    @Test
    void testToString()
    {
        String expectedStart = "DurationDiagramStatisticOuter[";
        String expectedContainsMetric = "metric=TIME_TO_PEAK_ERROR";
        String expectedContainsStatistic = "statistic=";
        String expectedContainsInstantAndDuration = "1970-01-02T10:17:36.000000789Z,PT13S";
        String expectedContainsMetadata = "metadata=PoolMetadata[";
        String expectedContainsMeasurementUnit = "measurementUnit=CMS";

        String m1ToString = DurationDiagramStatisticOuter.of( this.defaultInstance, this.metadata ).toString();
        assertTrue( m1ToString.startsWith( expectedStart ) );
        assertTrue( m1ToString.contains( expectedContainsMetric ) );
        assertTrue( m1ToString.contains( expectedContainsStatistic ) );
        assertTrue( m1ToString.contains( expectedContainsInstantAndDuration ) );
        assertTrue( m1ToString.contains( expectedContainsMetadata ) );
        assertTrue( m1ToString.contains( expectedContainsMeasurementUnit ) );
    }

    @Test
    void testGetMetadata()
    {
        assertEquals( PoolMetadata.of(),
                      DurationDiagramStatisticOuter.of( this.defaultInstance,
                                                        PoolMetadata.of() )
                                                   .getPoolMetadata() );
    }

    @Test
    void testHashCode()
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

        PoolMetadata m2 = PoolMetadata.of( evaluation, pool );

        DurationDiagramStatisticOuter anInstance = DurationDiagramStatisticOuter.of( this.defaultInstance, m2, 0.26 );

        // Equality and consistency
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( anInstance.hashCode(), anInstance.hashCode() );
        }
    }

    @Test
    void testExceptionOnConstructionWithNullData()
    {
        assertThrows( StatisticException.class, () -> DurationDiagramStatisticOuter.of( null, this.metadata ) );
    }

    @Test
    void testExceptionOnConstructionWithNullMetadata()
    {
        assertThrows( StatisticException.class, () -> DurationDiagramStatisticOuter.of( this.defaultInstance, null ) );
    }

    @Test
    void testGetSampleQuantile()
    {
        DurationDiagramStatisticOuter statistic = DurationDiagramStatisticOuter.of( this.defaultInstance,
                                                                                    PoolMetadata.of(),
                                                                                    0.26);

        assertEquals( 0.26, statistic.getSampleQuantile() );
    }
}

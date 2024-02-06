package wres.datamodel.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentType;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;

/**
 * Tests the {@link DiagramStatisticOuter}.
 *
 * @author James Brown
 */
class DiagramStatisticOuterTest
{
    private PoolMetadata metadata;

    private FeatureGroup featureGroup;

    private FeatureGroup anotherFeatureGroup;

    private DiagramMetricComponent podComponent;

    private DiagramMetricComponent pofdComponent;

    private DiagramMetric metric;

    private SummaryStatistic summaryStatistic;

    private DiagramStatisticOuter testInstance;

    @BeforeEach
    void runBeforeEachTest()
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

        Geometry anotherGeometry = wres.statistics.MessageFactory.getGeometry( "B" );
        GeometryTuple anotherGeoTuple = wres.statistics.MessageFactory.getGeometryTuple( anotherGeometry,
                                                                                         anotherGeometry,
                                                                                         anotherGeometry );
        GeometryGroup anotherGeoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, anotherGeoTuple );
        this.anotherFeatureGroup = FeatureGroup.of( anotherGeoGroup );

        Pool pool = MessageFactory.getPool( this.featureGroup,
                                            null,
                                            null,
                                            null,
                                            false,
                                            1 );

        this.metadata = PoolMetadata.of( evaluation, pool );

        this.podComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( MetricName.PROBABILITY_OF_DETECTION )
                                      .setType( DiagramComponentType.PRIMARY_RANGE_AXIS )
                                      .build();

        this.pofdComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( MetricName.PROBABILITY_OF_FALSE_DETECTION )
                                      .setType( DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                      .build();

        this.metric = DiagramMetric.newBuilder()
                                   .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                   .build();
        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( this.metric )
                                                  .build();

        this.summaryStatistic = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                .setDimension( SummaryStatistic.StatisticDimension.RESAMPLED )
                                                .setProbability( 0.27 )
                                                .build();

        this.testInstance = DiagramStatisticOuter.of( rocOne,
                                                      this.metadata,
                                                      this.summaryStatistic );
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

        Pool poolTwo = MessageFactory.getPool( this.anotherFeatureGroup,
                                               null,
                                               null,
                                               null,
                                               false,
                                               1 );

        PoolMetadata m3 = PoolMetadata.of( evaluation, poolTwo );

        DiagramStatisticComponent podTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocTwo = DiagramStatistic.newBuilder()
                                                  .addStatistics( podTwo )
                                                  .addStatistics( pofdTwo )
                                                  .setMetric( this.metric )
                                                  .build();

        DiagramStatisticComponent podThree =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) )
                                         .build();

        DiagramStatisticComponent pofdThree =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) )
                                         .build();

        DiagramStatistic rocThree = DiagramStatistic.newBuilder()
                                                    .addStatistics( podThree )
                                                    .addStatistics( pofdThree )
                                                    .setMetric( this.metric )
                                                    .build();

        DiagramStatisticOuter s = this.testInstance;
        DiagramStatisticOuter t = DiagramStatisticOuter.of( rocTwo, this.metadata, this.summaryStatistic );

        assertEquals( s, t );
        assertNotEquals( null, s );
        assertNotEquals( 1.0, s );
        assertNotEquals( s, DiagramStatisticOuter.of( rocThree, this.metadata, this.summaryStatistic ) );
        assertNotEquals( s, DiagramStatisticOuter.of( rocThree, m2, this.summaryStatistic ) );
        DiagramStatisticOuter q = DiagramStatisticOuter.of( s.getStatistic(), m2 );
        DiagramStatisticOuter r = DiagramStatisticOuter.of( rocTwo, m3 );
        assertEquals( q, q );
        assertNotEquals( s, r );
        assertNotEquals( r, s );
        assertNotEquals( q, r );
    }

    @Test
    void testGetMetadata()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.getPool( this.anotherFeatureGroup,
                                            null,
                                            null,
                                            null,
                                            false,
                                            1 );

        PoolMetadata m2 = PoolMetadata.of( evaluation, pool );

        DiagramStatisticOuter q = this.testInstance;
        DiagramStatisticOuter r = DiagramStatisticOuter.of( q.getStatistic(), m2 );

        assertNotEquals( q.getPoolMetadata(), r.getPoolMetadata() );
    }

    @Test
    void testHashCode()
    {
        DiagramStatisticComponent podTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocTwo = DiagramStatistic.newBuilder()
                                                  .addStatistics( podTwo )
                                                  .addStatistics( pofdTwo )
                                                  .setMetric( this.metric )
                                                  .build();

        DiagramStatisticOuter q = this.testInstance;
        DiagramStatisticOuter r = DiagramStatisticOuter.of( rocTwo, this.metadata, this.summaryStatistic );

        assertEquals( q.hashCode(), r.hashCode() );
    }

    @Test
    void testGetData()
    {
        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( this.pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( this.metric )
                                                  .build();

        assertEquals( rocOne, this.testInstance.getStatistic() );
    }

    @Test
    void testGetMetricComponentByType()
    {
        DiagramMetricComponent expected = this.podComponent;
        DiagramMetricComponent actual = this.testInstance.getComponent( DiagramComponentType.PRIMARY_RANGE_AXIS );
        assertEquals( expected, actual );
    }

    @Test
    void testGetSampleQuantile()
    {
        assertEquals( 0.27, this.testInstance.getSummaryStatistic()
                                             .getProbability() );
    }

    @Test
    void testExceptionOnNullData()
    {
        assertThrows( StatisticException.class, () -> DiagramStatisticOuter.of( null, this.metadata ) );
    }

    @Test
    void testExceptionOnNullMetadata()
    {
        DiagramStatistic statistic = DiagramStatistic.getDefaultInstance();

        assertThrows( StatisticException.class, () -> DiagramStatisticOuter.of( statistic, null ) );
    }

}

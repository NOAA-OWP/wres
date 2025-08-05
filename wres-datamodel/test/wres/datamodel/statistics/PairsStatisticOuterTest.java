package wres.datamodel.statistics;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.PairsMetric;
import wres.statistics.generated.PairsStatistic;
import wres.statistics.generated.Pool;
import wres.statistics.generated.ReferenceTime;

/**
 * Tests the {@link PairsStatisticOuter}.
 *
 * @author James Brown
 */
class PairsStatisticOuterTest
{
    private PoolMetadata metadata;

    private FeatureGroup featureGroup;

    private FeatureGroup anotherFeatureGroup;

    private PairsStatisticOuter testInstance;

    @BeforeEach
    void runBeforeEachTest()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Geometry geometry = MessageUtilities.getGeometry( "A" );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        this.featureGroup = FeatureGroup.of( geoGroup );

        Geometry anotherGeometry = MessageUtilities.getGeometry( "B" );
        GeometryTuple anotherGeoTuple = MessageUtilities.getGeometryTuple( anotherGeometry,
                                                                           anotherGeometry,
                                                                           anotherGeometry );
        GeometryGroup anotherGeoGroup = MessageUtilities.getGeometryGroup( null, anotherGeoTuple );
        this.anotherFeatureGroup = FeatureGroup.of( anotherGeoGroup );

        Pool pool = MessageFactory.getPool( this.featureGroup,
                                            null,
                                            null,
                                            null,
                                            false,
                                            1 );

        this.metadata = PoolMetadata.of( evaluation, pool );

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.TIME_SERIES_PLOT )
                                        .build();

        Timestamp firstTime = Timestamp.newBuilder()
                                       .setSeconds( 12300000 )
                                       .build();
        Timestamp secondTime = Timestamp.newBuilder()
                                        .setSeconds( 12303600 )
                                        .build();
        Timestamp thirdTime = Timestamp.newBuilder()
                                       .setSeconds( 12307200 )
                                       .build();
        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addReferenceTimes( ReferenceTime.newBuilder()
                                                                        .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                                                        .setReferenceTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 23.0 )
                                                            .addRight( 17.6 )
                                                            .setValidTime( secondTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 12.0 )
                                                            .addRight( 15.7 )
                                                            .setValidTime( thirdTime ) )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString() )
                           .addRightVariableNames( DatasetOrientation.RIGHT.toString() )
                           .addTimeSeries( timeSeries )
                           .build();

        PairsStatistic pairsStatistic = PairsStatistic.newBuilder()
                                                      .setStatistics( pairs )
                                                      .setMetric( metric )
                                                      .build();

        this.testInstance = PairsStatisticOuter.of( pairsStatistic,
                                                    this.metadata );
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

        Timestamp firstTime = Timestamp.newBuilder()
                                       .setSeconds( 12300000 )
                                       .build();
        Timestamp secondTime = Timestamp.newBuilder()
                                        .setSeconds( 12303600 )
                                        .build();
        Timestamp thirdTime = Timestamp.newBuilder()
                                       .setSeconds( 12307200 )
                                       .build();
        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addReferenceTimes( ReferenceTime.newBuilder()
                                                                        .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                                                        .setReferenceTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 23.0 )
                                                            .addRight( 17.6 )
                                                            .setValidTime( secondTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 12.0 )
                                                            .addRight( 15.7 )
                                                            .setValidTime( thirdTime ) )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString() )
                           .addRightVariableNames( DatasetOrientation.RIGHT.toString() )
                           .addTimeSeries( timeSeries )
                           .build();

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.TIME_SERIES_PLOT )
                                        .build();

        PairsStatistic pairsStatistic = PairsStatistic.newBuilder()
                                                      .setStatistics( pairs )
                                                      .setMetric( metric )
                                                      .build();

        Pairs.TimeSeriesOfPairs timeSeriesTwo =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addReferenceTimes( ReferenceTime.newBuilder()
                                                                        .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                                                        .setReferenceTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 23.0 )
                                                            .addRight( 17.6 )
                                                            .setValidTime( secondTime ) )
                                       .build();

        Pairs.TimeSeriesOfPairs timeSeriesThree =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addReferenceTimes( ReferenceTime.newBuilder()
                                                                        .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                                                        .setReferenceTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 28.0 )
                                                            .addRight( 16.6 )
                                                            .setValidTime( secondTime ) )
                                       .build();

        Pairs pairsTwo = Pairs.newBuilder()
                              .addLeftVariableNames( DatasetOrientation.LEFT.toString() )
                              .addRightVariableNames( DatasetOrientation.RIGHT.toString() )
                              .addTimeSeries( timeSeriesTwo )
                              .build();

        PairsStatistic pairsStatisticTwo = PairsStatistic.newBuilder()
                                                         .setStatistics( pairsTwo )
                                                         .setMetric( metric )
                                                         .build();

        Pairs pairsThree = Pairs.newBuilder()
                                .addLeftVariableNames( DatasetOrientation.LEFT.toString() )
                                .addRightVariableNames( DatasetOrientation.RIGHT.toString() )
                                .addTimeSeries( timeSeriesThree )
                                .build();

        PairsStatistic pairsStatisticThree = PairsStatistic.newBuilder()
                                                           .setStatistics( pairsThree )
                                                           .setMetric( metric )
                                                           .build();

        PairsStatisticOuter s = this.testInstance;
        PairsStatisticOuter t = PairsStatisticOuter.of( pairsStatistic, this.metadata );

        assertEquals( s, t );
        assertNotEquals( null, s );
        assertNotEquals( 1.0, s );
        assertNotEquals( s, PairsStatisticOuter.of( pairsStatisticThree, this.metadata ) );
        assertNotEquals( s, PairsStatisticOuter.of( pairsStatisticThree, m2 ) );
        PairsStatisticOuter q = PairsStatisticOuter.of( s.getStatistic(), m2 );
        PairsStatisticOuter r = PairsStatisticOuter.of( pairsStatisticTwo, m3 );
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

        PairsStatisticOuter q = this.testInstance;
        PairsStatisticOuter r = PairsStatisticOuter.of( q.getStatistic(), m2 );

        assertNotEquals( q.getPoolMetadata(), r.getPoolMetadata() );
    }

    @Test
    void testHashCode()
    {
        Timestamp firstTime = Timestamp.newBuilder()
                                       .setSeconds( 12300000 )
                                       .build();
        Timestamp secondTime = Timestamp.newBuilder()
                                        .setSeconds( 12303600 )
                                        .build();
        Timestamp thirdTime = Timestamp.newBuilder()
                                       .setSeconds( 12307200 )
                                       .build();
        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addReferenceTimes( ReferenceTime.newBuilder()
                                                                        .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                                                        .setReferenceTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 23.0 )
                                                            .addRight( 17.6 )
                                                            .setValidTime( secondTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 12.0 )
                                                            .addRight( 15.7 )
                                                            .setValidTime( thirdTime ) )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString() )
                           .addRightVariableNames( DatasetOrientation.RIGHT.toString() )
                           .addTimeSeries( timeSeries )
                           .build();

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.TIME_SERIES_PLOT )
                                        .build();

        PairsStatistic pairsStatistic = PairsStatistic.newBuilder()
                                                      .setStatistics( pairs )
                                                      .setMetric( metric )
                                                      .build();

        PairsStatisticOuter q = this.testInstance;
        PairsStatisticOuter r = PairsStatisticOuter.of( pairsStatistic, this.metadata );

        assertEquals( q.hashCode(), r.hashCode() );
    }

    @Test
    void testGetData()
    {
        Timestamp firstTime = Timestamp.newBuilder()
                                       .setSeconds( 12300000 )
                                       .build();
        Timestamp secondTime = Timestamp.newBuilder()
                                        .setSeconds( 12303600 )
                                        .build();
        Timestamp thirdTime = Timestamp.newBuilder()
                                       .setSeconds( 12307200 )
                                       .build();
        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addReferenceTimes( ReferenceTime.newBuilder()
                                                                        .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                                                        .setReferenceTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 23.0 )
                                                            .addRight( 17.6 )
                                                            .setValidTime( secondTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 12.0 )
                                                            .addRight( 15.7 )
                                                            .setValidTime( thirdTime ) )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString() )
                           .addRightVariableNames( DatasetOrientation.RIGHT.toString() )
                           .addTimeSeries( timeSeries )
                           .build();

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.TIME_SERIES_PLOT )
                                        .build();

        PairsStatistic pairsStatistic = PairsStatistic.newBuilder()
                                                      .setStatistics( pairs )
                                                      .setMetric( metric )
                                                      .build();

        assertEquals( pairsStatistic, this.testInstance.getStatistic() );
    }

    @Test
    void testExceptionOnNullData()
    {
        assertThrows( StatisticException.class, () -> PairsStatisticOuter.of( null, this.metadata ) );
    }

    @Test
    void testExceptionOnNullMetadata()
    {
        PairsStatistic statistic = PairsStatistic.getDefaultInstance();

        assertThrows( StatisticException.class, () -> PairsStatisticOuter.of( statistic, null ) );
    }

}

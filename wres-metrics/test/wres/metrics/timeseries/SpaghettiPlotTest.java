package wres.metrics.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.components.DatasetOrientation;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.types.Ensemble;
import wres.metrics.Boilerplate;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.PairsMetric;
import wres.statistics.generated.PairsStatistic;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link SpaghettiPlot}.
 *
 * @author James Brown
 */
class SpaghettiPlotTest
{
    /** Test instance. */
    private SpaghettiPlot plot;

    @BeforeEach
    void setupBeforeEachTest()
    {
        this.plot = SpaghettiPlot.of();
    }

    @Test
    void testApply()
    {
        // Generate some data
        Pool<TimeSeries<Pair<Double, Ensemble>>> input = MetricTestDataFactory.getTimeSeriesOfEnsemblePairsTwo();

        PairsStatisticOuter actual = this.plot.apply( input );

        TimeWindow inner = MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                                           Duration.ofHours( 24 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        FeatureGroup featureGroup = Boilerplate.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "MAP" )
                                          .setMeasurementUnit( "MM/DAY" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      window,
                                                                      null,
                                                                      null,
                                                                      false );

        PoolMetadata expectedMetadata = PoolMetadata.of( evaluation, pool );


        Timestamp firstTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-13T00:00:00Z" ) );
        Timestamp secondTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-13T06:00:00Z" ) );
        Timestamp thirdTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-13T12:00:00Z" ) );

        Timestamp firstReferenceTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-12T18:00:00Z" ) );
        ReferenceTime one = ReferenceTime.newBuilder()
                                         .setReferenceTime( firstReferenceTime )
                                         .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                         .build();

        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 22.9 )
                                                            .addRight( 22.8 )
                                                            .addRight( 23.9 )
                                                            .setValidTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 26.4 )
                                                            .addRight( 23.8 )
                                                            .addRight( 23.7 )
                                                            .setValidTime( secondTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 23.2 )
                                                            .addRight( 16.1 )
                                                            .addRight( 18.4 )
                                                            .setValidTime( thirdTime ) )
                                       .addReferenceTimes( one )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString()
                                                                         .toUpperCase() )
                           .addRightVariableNames( "1" )
                           .addRightVariableNames( "2" )
                           .addTimeSeries( timeSeries )
                           .build();

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.SPAGHETTI_PLOT )
                                        .setUnits( "MM/DAY" )
                                        .build();

        PairsStatistic expectedPairsStatistic = PairsStatistic.newBuilder()
                                                              .setStatistics( pairs )
                                                              .setMetric( metric )
                                                              .build();

        PairsStatisticOuter expected = PairsStatisticOuter.of( expectedPairsStatistic, expectedMetadata );

        assertEquals( expected, actual );
    }

    @Test
    void testApplyWithUnevenMembers()
    {
        // See GitHub #711

        // Generate some data
        Pool<TimeSeries<Pair<Double, Ensemble>>> input = MetricTestDataFactory.getTimeSeriesOfEnsemblePairsThree();

        PairsStatisticOuter actual = this.plot.apply( input );

        TimeWindow inner = MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                                           Duration.ofHours( 24 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        FeatureGroup featureGroup = Boilerplate.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "MAP" )
                                          .setMeasurementUnit( "MM/DAY" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      window,
                                                                      null,
                                                                      null,
                                                                      false );

        PoolMetadata expectedMetadata = PoolMetadata.of( evaluation, pool );


        Timestamp firstTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-13T00:00:00Z" ) );
        Timestamp secondTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-13T06:00:00Z" ) );
        Timestamp thirdTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-13T12:00:00Z" ) );

        Timestamp firstReferenceTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-12T18:00:00Z" ) );
        ReferenceTime one = ReferenceTime.newBuilder()
                                         .setReferenceTime( firstReferenceTime )
                                         .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                         .build();

        Timestamp secondReferenceTime = MessageUtilities.getTimestamp( Instant.parse( "1985-03-13T18:00:00Z" ) );
        ReferenceTime two = ReferenceTime.newBuilder()
                                         .setReferenceTime( secondReferenceTime )
                                         .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                         .build();

        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 22.9 )
                                                            .addRight( 22.8 )
                                                            .addRight( 23.9 )
                                                            .addRight( Double.NaN )
                                                            .setValidTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 23.2 )
                                                            .addRight( 16.1 )
                                                            .addRight( 18.4 )
                                                            .addRight( Double.NaN )
                                                            .setValidTime( thirdTime ) )
                                       .addReferenceTimes( one )
                                       .build();

        Pairs.TimeSeriesOfPairs timeSeriesTwo =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 26.4 )
                                                            .addRight( 23.8 )
                                                            .addRight( 23.7 )
                                                            .addRight( 32.5 )
                                                            .setValidTime( secondTime ) )
                                       .addReferenceTimes( two )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString()
                                                                         .toUpperCase() )
                           .addAllRightVariableNames( List.of( "1", "2", "3" ) )
                           .addTimeSeries( timeSeries )
                           .addTimeSeries( timeSeriesTwo )
                           .build();

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.SPAGHETTI_PLOT )
                                        .setUnits( "MM/DAY" )
                                        .build();

        PairsStatistic expectedPairsStatistic = PairsStatistic.newBuilder()
                                                              .setStatistics( pairs )
                                                              .setMetric( metric )
                                                              .build();

        PairsStatisticOuter expected = PairsStatisticOuter.of( expectedPairsStatistic, expectedMetadata );

        assertEquals( expected, actual );
    }

    @Test
    void testHasRealUnitsReturnsTrue()
    {
        assertTrue( this.plot.hasRealUnits() );
    }

    @Test
    void testGetName()
    {
        assertEquals( MetricConstants.SPAGHETTI_PLOT, this.plot.getMetricName() );
    }

    @Test
    void testApplyThrowsExceptionOnNullInput()
    {
        assertThrows( PoolException.class, () -> this.plot.apply( null ) );
    }
}

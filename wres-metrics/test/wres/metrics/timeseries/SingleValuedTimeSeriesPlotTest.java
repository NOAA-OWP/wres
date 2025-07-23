package wres.metrics.timeseries;

import java.time.Duration;
import java.time.Instant;

import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
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
 * Tests the {@link SingleValuedTimeSeriesPlot}.
 *
 * @author James Brown
 */
class SingleValuedTimeSeriesPlotTest
{
    /** Test instance. */
    private SingleValuedTimeSeriesPlot plot;

    @BeforeEach
    void setupBeforeEachTest()
    {
        this.plot = SingleValuedTimeSeriesPlot.of();
    }

    @Test
    void testApply()
    {
        // Generate some data
        Pool<TimeSeries<Pair<Double, Double>>> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        PairsStatisticOuter actual = this.plot.apply( input );

        // Create some default metadata for the time-series
        TimeWindow inner = MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 18 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup( "A", false );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "Streamflow" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      window,
                                                                      null,
                                                                      null,
                                                                      false );

        PoolMetadata expectedMetadata = PoolMetadata.of( evaluation, pool );

        Timestamp firstTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-01T06:00:00Z" ) );
        Timestamp secondTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-01T12:00:00Z" ) );
        Timestamp thirdTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-01T18:00:00Z" ) );

        Timestamp firstReferenceTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-01T00:00:00Z" ) );
        ReferenceTime one = ReferenceTime.newBuilder()
                                         .setReferenceTime( firstReferenceTime )
                                         .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                         .build();

        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 1.0 )
                                                            .addRight( 1.0 )
                                                            .setValidTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 1.0 )
                                                            .addRight( 5.0 )
                                                            .setValidTime( secondTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 5.0 )
                                                            .addRight( 1.0 )
                                                            .setValidTime( thirdTime ) )
                                       .addReferenceTimes( one )
                                       .build();

        Timestamp fourthTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-02T06:00:00Z" ) );
        Timestamp fifthTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-02T12:00:00Z" ) );
        Timestamp sixthTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-02T18:00:00Z" ) );

        Timestamp secondReferenceTime = MessageUtilities.getTimestamp( Instant.parse( "1985-01-02T00:00:00Z" ) );

        ReferenceTime two = ReferenceTime.newBuilder()
                                         .setReferenceTime( secondReferenceTime )
                                         .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                         .build();

        Pairs.TimeSeriesOfPairs secondTimeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 10.0 )
                                                            .addRight( 1.0 )
                                                            .setValidTime( fourthTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 1.0 )
                                                            .addRight( 1.0 )
                                                            .setValidTime( fifthTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 1.0 )
                                                            .addRight( 10.0 )
                                                            .setValidTime( sixthTime ) )
                                       .addReferenceTimes( two )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString()
                                                                         .toUpperCase() )
                           .addRightVariableNames( DatasetOrientation.RIGHT.toString()
                                                                           .toUpperCase() )
                           .addTimeSeries( timeSeries )
                           .addTimeSeries( secondTimeSeries )
                           .build();

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.TIME_SERIES_PLOT )
                                        .setUnits( "CMS" )
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
        assertEquals( MetricConstants.TIME_SERIES_PLOT, this.plot.getMetricName() );
    }

    @Test
    void testApplyThrowsExceptionOnNullInput()
    {
        assertThrows( PoolException.class, () -> this.plot.apply( null ) );
    }
}

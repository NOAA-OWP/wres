package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.pools.pairs.CrossPairs;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.generators.PersistenceGenerator;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MeanSquareErrorSkillScore}.
 * 
 * @author James Brown
 */
public final class MeanSquareErrorSkillScoreTest
{
    /**
     * Default instance of a {@link MeanSquareErrorSkillScore}.
     */

    private MeanSquareErrorSkillScore msess;

    @Before
    public void setupBeforeEachTest()
    {
        this.msess = MeanSquareErrorSkillScore.of();
    }

    @Test
    public void testApplyWithBaseline()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( 0.8007025335093799 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithoutBaseline() throws IOException
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( 0.7832791707526114 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithoutBaselineTwo()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( 0.9963647159052861 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    /**
     * Integration test with a persistence baseline generated from a raw time-series. See issue #99160. This tests a 
     * variant of the MSESS as defined by Kitanidis, P.K., and Bras, R.L. 1980. Real-time forecasting with a conceptual 
     * hydrologic model. 2. Applications and results. Water Resources Research, Vol. 16, No. 6, pp. 1034:1044. In that 
     * context, it is known as the Coefficient of Persistence and ignores the first pair in the numerator, which is 
     * essentially equivalent to cross-pairing the dense series of pairs before computing the MSESS relative to the 
     * lag-1 persistence.
     */

    @Test
    public void testApplyAsCoefficientOfPersistence()
    {
        // Create the raw time-series
        FeatureKey feature = FeatureKey.of( "FAKE2" );

        TimeSeriesMetadata observedMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                     TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                                        TimeScaleFunction.MEAN ),
                                                                     "STREAMFLOW",
                                                                     feature,
                                                                     "CMS" );

        TimeSeries<Double> observed =
                new Builder<Double>().addEvent( Event.of( Instant.parse( "2551-03-18T13:00:00Z" ), 2.1 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T14:00:00Z" ), 6.5 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), 3.12 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T16:00:00Z" ), 5.89 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T17:00:00Z" ), 7.32 ) )
                                     .setMetadata( observedMetadata )
                                     .build();

        TimeSeriesMetadata simulationMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                       TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                                          TimeScaleFunction.MEAN ),
                                                                       "STREAMFLOW",
                                                                       feature,
                                                                       "CMS" );

        TimeSeries<Double> simulated =
                new Builder<Double>().addEvent( Event.of( Instant.parse( "2551-03-18T13:00:00Z" ), 1.9 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T14:00:00Z" ), 6.32 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), 3.2 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T16:00:00Z" ), 5.4 ) )
                                     .addEvent( Event.of( Instant.parse( "2551-03-18T17:00:00Z" ), 6.9 ) )
                                     .setMetadata( simulationMetadata )
                                     .build();

        // Create the persistence/generated time-series
        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( observed ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite );

        TimeSeries<Double> persistence = generator.apply( simulated );

        // Pair
        TimeSeriesPairerByExactTime<Double, Double> pairer = TimeSeriesPairerByExactTime.of();

        TimeSeries<Pair<Double, Double>> main = pairer.pair( observed, simulated );
        TimeSeries<Pair<Double, Double>> baseline = pairer.pair( observed, persistence );

        TimeSeriesCrossPairer<Double, Double> p = TimeSeriesCrossPairer.of();

        // Cross-pair
        CrossPairs<Double, Double> cp = p.apply( List.of( main ), List.of( baseline ) );
        main = cp.getMainPairs()
                 .get( 0 );
        baseline = cp.getBaselinePairs()
                     .get( 0 );

        Pool<TimeSeries<Pair<Double, Double>>> mainPool = Pool.of( List.of( main ), PoolMetadata.of() );
        Pool<TimeSeries<Pair<Double, Double>>> basePool = Pool.of( List.of( baseline ), PoolMetadata.of() );

        Pool<Pair<Double, Double>> mainNoSeries = PoolSlicer.unpack( mainPool );
        Pool<Pair<Double, Double>> baseNoSeries = PoolSlicer.unpack( basePool );

        Pool<Pair<Double, Double>> combined = Pool.of( mainNoSeries.get(),
                                                       PoolMetadata.of(),
                                                       baseNoSeries.get(),
                                                       PoolMetadata.of(),
                                                       null );

        DoubleScoreStatisticOuter actual = this.msess.apply( combined );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( 0.9887586353333894 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.toString(), this.msess.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.msess.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( this.msess.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.msess.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE, this.msess.getCollectionOf() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected = assertThrows( PoolException.class, () -> this.msess.apply( null ) );

        assertEquals( "Specify non-null input to the 'MEAN SQUARE ERROR SKILL SCORE'.", expected.getMessage() );
    }

}

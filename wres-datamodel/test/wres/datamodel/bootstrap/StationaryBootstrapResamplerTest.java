package wres.datamodel.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well512a;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.statistics.MessageFactory;
import wres.statistics.generated.ReferenceTime;

/**
 * Tests the {@link StationaryBootstrapResampler}.
 * @author James Brown
 */
class StationaryBootstrapResamplerTest
{
    @Test
    void testResampleProducesExpectedPoolShapeForPoolWithOneSeries()
    {
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 5.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 6.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 7.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 8.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T01:00:00Z" ), 9.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T02:00:00Z" ), 10.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( null ) )
                        .build();

        Pool<TimeSeries<Double>> pool = Pool.of( List.of( one ), PoolMetadata.of() );

        RandomGenerator randomGenerator = new Well512a();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator,
                                                                                          ForkJoinPool.commonPool() );
        Pool<TimeSeries<Double>> actual = resampler.resample();

        assertAll( () -> assertEquals( 1, pool.get()
                                              .size() ),
                   () -> assertEquals( 10, actual.get()
                                                 .get( 0 )
                                                 .getEvents()
                                                 .size() ) );
    }

    @Test
    void testResampleProducesExpectedPoolShapeWhenPoolContainsBaseline()
    {
        Instant first = Instant.parse( "1988-10-04T16:00:00Z" );
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        Instant second = Instant.parse( "1988-10-04T16:00:00Z" );
        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 5.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        Pool<TimeSeries<Double>> pool = Pool.of( List.of( one, one ),
                                                 PoolMetadata.of(),
                                                 List.of( two, two ),
                                                 PoolMetadata.of( true ),
                                                 null );

        RandomGenerator randomGenerator = new Well512a();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator,
                                                                                          ForkJoinPool.commonPool() );
        Pool<TimeSeries<Double>> actual = resampler.resample();

        assertAll( () -> assertEquals( 2, pool.get()
                                              .size() ),
                   () -> assertEquals( 2, pool.getBaselineData()
                                              .get()
                                              .size() ),
                   () -> assertEquals( 2, actual.get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 2, actual.getBaselineData()
                                                .get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 2, actual.get()
                                                .get( 1 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 2, actual.getBaselineData()
                                                .get()
                                                .get( 1 )
                                                .getEvents()
                                                .size() ) );
    }


    @Test
    void testResampleProducesExpectedPoolShapeForPoolWithMultipleSeriesContainingSingleEvent()
    {
        Instant first = Instant.parse( "1988-10-04T16:00:00Z" );
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        Instant second = Instant.parse( "1988-10-04T19:00:00Z" );
        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 4.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        Instant third = Instant.parse( "1988-10-04T22:00:00Z" );
        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 8.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        Pool<TimeSeries<Double>> pool = Pool.of( List.of( one, two, three ), PoolMetadata.of() );

        RandomGenerator randomGenerator = new Well512a();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator,
                                                                                          ForkJoinPool.commonPool() );
        Pool<TimeSeries<Double>> actual = resampler.resample();

        assertAll( () -> assertEquals( 3, pool.get()
                                              .size() ),
                   () -> assertEquals( 1, actual.get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 1, actual.get()
                                                .get( 1 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 1, actual.get()
                                                .get( 2 )
                                                .getEvents()
                                                .size() ) );
    }

    @Test
    void testResampleProducesExpectedPoolShapeForPoolWithMultipleSeriesOfDifferentLengths()
    {
        Instant first = Instant.parse( "1988-10-04T16:00:00Z" );
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 3.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        Instant second = Instant.parse( "1988-10-04T18:00:00Z" );
        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 5.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 6.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 7.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        Instant third = Instant.parse( "1988-10-04T20:00:00Z" );
        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 8.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 9.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 10.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 11.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        Pool<TimeSeries<Double>> pool = Pool.of( List.of( one, two, three ), PoolMetadata.of() );

        RandomGenerator randomGenerator = new Well512a();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator,
                                                                                          ForkJoinPool.commonPool() );
        Pool<TimeSeries<Double>> actual = resampler.resample();

        assertAll( () -> assertEquals( 3, pool.get()
                                              .size() ),
                   () -> assertEquals( 3, actual.get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 4, actual.get()
                                                .get( 1 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 4, actual.get()
                                                .get( 2 )
                                                .getEvents()
                                                .size() ) );
    }

    @Test
    void testResampleContainsExpectedValuesForPoolWithMultipleSeriesOfDifferentLengths()
    {
        Instant first = Instant.parse( "1988-10-04T16:00:00Z" );
        TimeSeries<Double> oneOne =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 1.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        Instant second = Instant.parse( "1988-10-04T18:00:00Z" );
        TimeSeries<Double> oneTwo =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 2.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 2.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 2.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        Instant third = Instant.parse( "1988-10-04T20:00:00Z" );
        TimeSeries<Double> twoOne =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 3.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        Instant fourth = Instant.parse( "1988-10-04T22:00:00Z" );
        TimeSeries<Double> twoTwo =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T01:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T02:00:00Z" ), 4.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( fourth ) )
                        .build();

        Pool<TimeSeries<Double>> pool = Pool.of( List.of( oneOne, oneTwo, twoOne, twoTwo ), PoolMetadata.of() );

        // Fix the seed to generate an expected sequence
        RandomGenerator randomGenerator = new Well512a( 123456 );

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          1,
                                                                                          randomGenerator,
                                                                                          ForkJoinPool.commonPool() );

        // The time-series with 3 events can contain event values with any of numbers (1,2,3,4). The time-series with
        // 4 events can only contain event values with numbers (3,4)
        Pool<TimeSeries<Double>> actual = resampler.resample();

        TimeSeries<Double> oneOneExpected =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 4.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        TimeSeries<Double> oneTwoExpected =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 2.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 3.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        TimeSeries<Double> twoOneExpected =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 3.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        TimeSeries<Double> twoTwoExpected =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T01:00:00Z" ), 3.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T02:00:00Z" ), 4.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( fourth ) )
                        .build();

        Pool<TimeSeries<Double>> expected = Pool.of( List.of( oneOneExpected,
                                                              oneTwoExpected,
                                                              twoOneExpected,
                                                              twoTwoExpected ),
                                                     PoolMetadata.of() );

        assertEquals( expected, actual );
    }
    
    @Test
    void testResampleProducesExpectedPoolShapeWhenPoolContainsMinipools()
    {
        Instant first = Instant.parse( "1988-10-04T16:00:00Z" );
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        Instant second = Instant.parse( "1988-10-04T19:00:00Z" );
        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 5.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        Pool<TimeSeries<Double>> poolOne = Pool.of( List.of( one, two ),
                                                    PoolMetadata.of() );

        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 11.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 12.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        TimeSeries<Double> four =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 14.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 15.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        Pool<TimeSeries<Double>> poolTwo = Pool.of( List.of( three, four ),
                                                    PoolMetadata.of() );

        Pool<TimeSeries<Double>> pool = new Pool.Builder<TimeSeries<Double>>().addPool( poolOne )
                                                                              .addPool( poolTwo )
                                                                              .build();

        RandomGenerator randomGenerator = new Well512a();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator,
                                                                                          ForkJoinPool.commonPool() );
        Pool<TimeSeries<Double>> reampled = resampler.resample();
        List<Pool<TimeSeries<Double>>> actual = reampled.getMiniPools();

        assertAll( () -> assertEquals( 2, actual
                           .size() ),
                   () -> assertEquals( 2, actual.get( 0 )
                                                .get()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 0 )
                                                .get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 0 )
                                                .get()
                                                .get( 1 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 1 )
                                                .get()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 1 )
                                                .get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 1 )
                                                .get()
                                                .get( 1 )
                                                .getEvents()
                                                .size() ) );
    }

    @Test
    void testResampleProducesExpectedPoolShapeWhenPoolContainsMinipoolsAndBaselineWithDifferentShapes()
    {
        Instant first = Instant.parse( "1988-10-04T16:00:00Z" );
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( first ) )
                        .build();

        Instant second = Instant.parse( "1988-10-04T19:00:00Z" );
        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 5.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 6.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();
        Instant third = Instant.parse( "1988-10-04T22:00:00Z" );
        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 8.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        TimeSeries<Double> baseOne =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 14.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 15.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 16.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        TimeSeries<Double> baseTwo =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 9.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        Pool<TimeSeries<Double>> poolOne = Pool.of( List.of( one, two, three ),
                                                    PoolMetadata.of(),
                                                    List.of( baseOne, baseTwo ),
                                                    PoolMetadata.of( true ),
                                                    null );

        TimeSeries<Double> four =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 11.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 12.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        TimeSeries<Double> five =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 14.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 15.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        TimeSeries<Double> baseThree =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 24.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 25.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( second ) )
                        .build();

        TimeSeries<Double> baseFour =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 26.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 27.0 ) )
                        .setMetadata( StationaryBootstrapResamplerTest.getBoilerplateMetadata( third ) )
                        .build();

        Pool<TimeSeries<Double>> poolTwo = Pool.of( List.of( four, five ),
                                                    PoolMetadata.of(),
                                                    List.of( baseThree, baseFour ),
                                                    PoolMetadata.of( true ),
                                                    null );

        Pool<TimeSeries<Double>> pool = new Pool.Builder<TimeSeries<Double>>().addPool( poolOne )
                                                                              .addPool( poolTwo )
                                                                              .build();

        RandomGenerator randomGenerator = new Well512a();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator,
                                                                                          ForkJoinPool.commonPool() );
        Pool<TimeSeries<Double>> reampled = resampler.resample();
        List<Pool<TimeSeries<Double>>> actual = reampled.getMiniPools();

        assertAll( () -> assertEquals( 2, actual
                           .size() ),
                   () -> assertEquals( 2, actual.get( 0 )
                                                .get()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 0 )
                                                .getBaselineData()
                                                .get()
                                                .size() ),
                   () -> assertEquals( 1, actual.get( 0 )
                                                .get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 1, actual.get( 0 )
                                                .getBaselineData()
                                                .get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 1 )
                                                .get()
                                                .size() ),
                   () -> assertEquals( 2, actual.get( 1 )
                                                .getBaselineData()
                                                .get()
                                                .size() ),
                   () -> assertEquals( 1, actual.get( 1 )
                                                .get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ),
                   () -> assertEquals( 1, actual.get( 1 )
                                                .getBaselineData()
                                                .get()
                                                .get( 0 )
                                                .getEvents()
                                                .size() ) );
    }

    /**
     * @return some boilerplate metadata
     */

    private static TimeSeriesMetadata getBoilerplateMetadata( Instant referenceTime )
    {
        Map<ReferenceTime.ReferenceTimeType, Instant> referenceTimes =
                new EnumMap<>( ReferenceTime.ReferenceTimeType.class );
        if ( Objects.nonNull( referenceTime ) )
        {
            referenceTimes.put( ReferenceTime.ReferenceTimeType.T0, referenceTime );
        }
        return TimeSeriesMetadata.of( referenceTimes,
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      "foo",
                                      Feature.of( MessageFactory.getGeometry( "bar" ) ),
                                      "baz" );
    }
}

package wres.datamodel.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
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

        RandomGenerator randomGenerator = new MersenneTwister();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator );

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

        RandomGenerator randomGenerator = new MersenneTwister();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator );

        Pool<TimeSeries<Double>> actual = resampler.resample();

        assertAll( () -> assertEquals( 1, pool.get()
                                              .size() ),
                   () -> assertEquals( 10, actual.get()
                                                 .get( 0 )
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

        RandomGenerator randomGenerator = new MersenneTwister();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator );

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
    void testResampleProducesExpectedPoolShapeWhenPoolContainsBaseline()
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

        Pool<TimeSeries<Double>> pool = Pool.of( List.of( one ),
                                                 PoolMetadata.of(),
                                                 List.of( two ),
                                                 PoolMetadata.of( true ),
                                                 null );

        RandomGenerator randomGenerator = new MersenneTwister();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator );

        Pool<TimeSeries<Double>> actual = resampler.resample();

        assertAll( () -> assertEquals( 1, pool.get()
                                              .size() ),
                   () -> assertEquals( 1, pool.getBaselineData()
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
                                                .size() ) );
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

        RandomGenerator randomGenerator = new MersenneTwister();

        StationaryBootstrapResampler<Double> resampler = StationaryBootstrapResampler.of( pool,
                                                                                          2,
                                                                                          randomGenerator );

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

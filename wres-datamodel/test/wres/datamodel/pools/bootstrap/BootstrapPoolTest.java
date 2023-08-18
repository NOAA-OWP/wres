package wres.datamodel.pools.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

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

/**
 * Tests the {@link BootstrapPool}.
 * @author James Brown
 */
class BootstrapPoolTest
{
    @Test
    void testGetTimeSeriesWithAtLeastThisManyEvents()
    {
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 3.0 ) )
                        .setMetadata( BootstrapPoolTest.getBoilerplateMetadata() )
                        .build();

        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 4.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T20:00:00Z" ), 5.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 6.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 7.0 ) )
                        .setMetadata( BootstrapPoolTest.getBoilerplateMetadata() )
                        .build();

        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 8.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T22:00:00Z" ), 9.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 10.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 11.0 ) )
                        .setMetadata( BootstrapPoolTest.getBoilerplateMetadata() )
                        .build();

        TimeSeries<Double> four =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ), 12.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 13.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T01:00:00Z" ), 14.0 ) )
                        .setMetadata( BootstrapPoolTest.getBoilerplateMetadata() )
                        .build();

        TimeSeries<Double> five =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-05T01:00:00Z" ), 15.0 ) )
                        .setMetadata( BootstrapPoolTest.getBoilerplateMetadata() )
                        .build();

        Pool<TimeSeries<Double>> pool = Pool.of( List.of( one, two, three, four, five ), PoolMetadata.of() );

        BootstrapPool<Double> bootstrapPool = BootstrapPool.of( pool );

        assertAll( () -> assertEquals( 5,
                                       bootstrapPool.getTimeSeriesWithAtLeastThisManyEvents( 1 )
                                                    .size() ),
                   () -> assertEquals( 4,
                                       bootstrapPool.getTimeSeriesWithAtLeastThisManyEvents( 3 )
                                                    .size() ),
                   () -> assertEquals( 2,
                                       bootstrapPool.getTimeSeriesWithAtLeastThisManyEvents( 4 )
                                                    .size() ) );
    }

    /**
     * @return some boilerplate metadata
     */

    private static TimeSeriesMetadata getBoilerplateMetadata()
    {
        return TimeSeriesMetadata.of( Map.of(),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      "foo",
                                      Feature.of( MessageFactory.getGeometry( "bar" ) ),
                                      "baz" );
    }
}

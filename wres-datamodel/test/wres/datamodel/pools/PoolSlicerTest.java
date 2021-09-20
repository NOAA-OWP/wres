package wres.datamodel.pools;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;

/**
 * Tests the {@link PoolSlicer}.
 * 
 * @author James Brown
 */

class PoolSlicerTest
{

    @Test
    void testGetPairCount()
    {
        Pool<TimeSeries<Boolean>> pool = Pool.of( List.of(), PoolMetadata.of() );

        assertEquals( 0, PoolSlicer.getPairCount( pool ) );


        SortedSet<Event<Boolean>> eventsOne = new TreeSet<>();
        eventsOne.add( Event.of( Instant.MIN, Boolean.valueOf( false ) ) );
        SortedSet<Event<Boolean>> eventsTwo = new TreeSet<>();
        eventsTwo.add( Event.of( Instant.MIN, Boolean.valueOf( false ) ) );
        eventsTwo.add( Event.of( Instant.MAX, Boolean.valueOf( true ) ) );

        Pool<TimeSeries<Boolean>> anotherPool =
                Pool.of( List.of( TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "foo",
                                                                        FeatureKey.of( "bar" ),
                                                                        "baz" ),
                                                 eventsOne ),
                                  TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "bla",
                                                                        FeatureKey.of( "smeg" ),
                                                                        "faz" ),
                                                 eventsTwo ) ),
                         PoolMetadata.of() );

        assertEquals( 3, PoolSlicer.getPairCount( anotherPool ) );
    }

    @Test
    void testUnpack()
    {
        SortedSet<Event<String>> eventsOne = new TreeSet<>();
        eventsOne.add( Event.of( Instant.MIN, "Un" ) );
        SortedSet<Event<String>> eventsTwo = new TreeSet<>();
        eventsTwo.add( Event.of( Instant.MIN, "pack" ) );
        eventsTwo.add( Event.of( Instant.MAX, "ed!" ) );

        Pool<TimeSeries<String>> pool =
                Pool.of( List.of( TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "foo",
                                                                        FeatureKey.of( "bar" ),
                                                                        "baz" ),
                                                 eventsOne ),
                                  TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "bla",
                                                                        FeatureKey.of( "smeg" ),
                                                                        "faz" ),
                                                 eventsTwo ) ),
                         PoolMetadata.of() );

        Pool<String> expected = Pool.of( List.of( "Un", "pack", "ed!" ), PoolMetadata.of() );
        
        assertEquals( expected, PoolSlicer.unpack( pool ) );
    }


}

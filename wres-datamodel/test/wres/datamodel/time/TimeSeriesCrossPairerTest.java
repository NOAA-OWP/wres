package wres.datamodel.time;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.sampledata.pairs.CrossPairs;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Tests the {@link Event}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesCrossPairerTest
{

    /**
     * An instance to test.
     */

    private TimeSeriesCrossPairer<Integer, Integer> instance;

    @Before
    public void runBeforeEachTest()
    {
        this.instance = TimeSeriesCrossPairer.of();
    }

    @Test
    public void testCrossPairTwoSeriesWithEqualReferenceTimesThatEachAppearTwice()
    {
        Event<Pair<Integer, Integer>> first = Event.of( Instant.parse( "2123-12-01T06:00:00Z" ), Pair.of( 1, 1 ) );
        Event<Pair<Integer, Integer>> second = Event.of( Instant.parse( "2123-12-01T12:00:00Z" ), Pair.of( 2, 2 ) );
        Event<Pair<Integer, Integer>> third = Event.of( Instant.parse( "2123-12-01T18:00:00Z" ), Pair.of( 3, 3 ) );

        Event<Pair<Integer, Integer>> fourth = Event.of( Instant.parse( "2123-12-01T19:00:00Z" ), Pair.of( 4, 4 ) );
        Event<Pair<Integer, Integer>> fifth = Event.of( Instant.parse( "2123-12-01T20:00:00Z" ), Pair.of( 5, 5 ) );

        Event<Pair<Integer, Integer>> sixth = Event.of( Instant.parse( "2123-12-01T06:00:00Z" ), Pair.of( 6, 6 ) );
        Event<Pair<Integer, Integer>> seventh = Event.of( Instant.parse( "2123-12-01T12:00:00Z" ), Pair.of( 7, 7 ) );
        Event<Pair<Integer, Integer>> eighth = Event.of( Instant.parse( "2123-12-01T18:00:00Z" ), Pair.of( 8, 8 ) );

        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 Instant.parse( "2123-12-01T00:00:00Z" ) ) );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                               .addEvent( first )
                                                               .addEvent( second )
                                                               .addEvent( third )
                                                               .addEvent( fourth )
                                                               .build();

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                               .addEvent( sixth )
                                                               .addEvent( seventh )
                                                               .addEvent( eighth )
                                                               .addEvent( fifth )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                this.instance.apply( List.of( firstSeries, firstSeries ), List.of( secondSeries, secondSeries ) );

        TimeSeries<Pair<Integer, Integer>> expectedSeriesMain =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                               .addEvent( first )
                                                               .addEvent( second )
                                                               .addEvent( third )
                                                               .build();

        TimeSeries<Pair<Integer, Integer>> expectedSeriesBase =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                               .addEvent( sixth )
                                                               .addEvent( seventh )
                                                               .addEvent( eighth )
                                                               .build();

        CrossPairs<Integer, Integer> expected =
                CrossPairs.of( List.of( expectedSeriesMain, expectedSeriesMain ),
                               List.of( expectedSeriesBase, expectedSeriesBase ) );

        assertEquals( expected, actual );

    }

}

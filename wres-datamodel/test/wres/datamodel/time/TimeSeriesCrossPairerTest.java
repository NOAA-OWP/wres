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
import wres.datamodel.time.TimeSeriesCrossPairer.MatchMode;

/**
 * Tests the {@link Event}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesCrossPairerTest
{

    private static final Instant ZEROTH = Instant.parse( "2123-12-01T00:00:00Z" );
    private static final Instant FIRST = Instant.parse( "2123-12-01T06:00:00Z" );
    private static final Instant SECOND = Instant.parse( "2123-12-01T12:00:00Z" );
    private static final Instant THIRD = Instant.parse( "2123-12-01T18:00:00Z" );
    private static final Instant FOURTH = Instant.parse( "2123-12-01T19:00:00Z" );
    private static final Instant FIFTH = Instant.parse( "2123-12-01T20:00:00Z" );
    private static final Instant SIXTH = Instant.parse( "2123-12-01T06:00:00Z" );
    private static final Instant SEVENTH = Instant.parse( "2123-12-01T12:00:00Z" );
    private static final Instant EIGHTH = Instant.parse( "2123-12-01T18:00:00Z" );

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
    public void testCrossPairTwoTimeSeriesWithEqualReferenceTimesThatEachAppearTwice()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );
        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );
        Event<Pair<Integer, Integer>> third = Event.of( THIRD, Pair.of( 3, 3 ) );

        Event<Pair<Integer, Integer>> fourth = Event.of( FOURTH, Pair.of( 4, 4 ) );
        Event<Pair<Integer, Integer>> fifth = Event.of( FIFTH, Pair.of( 5, 5 ) );

        Event<Pair<Integer, Integer>> sixth = Event.of( SIXTH, Pair.of( 6, 6 ) );
        Event<Pair<Integer, Integer>> seventh = Event.of( SEVENTH, Pair.of( 7, 7 ) );
        Event<Pair<Integer, Integer>> eighth = Event.of( EIGHTH, Pair.of( 8, 8 ) );

        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

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

    @Test
    public void testCrossPairTimeSeriesWithSomeEqualReferenceTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                               .addEvent( first )
                                                               .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ) );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .build();

        Event<Pair<Integer, Integer>> third = Event.of( FIRST, Pair.of( 3, 3 ) );

        TimeSeriesMetadata thirdMetadata = firstMetadata;

        TimeSeries<Pair<Integer, Integer>> thirdSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( thirdMetadata )
                                                               .addEvent( third )
                                                               .build();

        Event<Pair<Integer, Integer>> fourth = Event.of( FOURTH, Pair.of( 4, 4 ) );

        TimeSeriesMetadata fourthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 SECOND ) );

        TimeSeries<Pair<Integer, Integer>> fourthSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( fourthMetadata )
                                                               .addEvent( fourth )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                this.instance.apply( List.of( firstSeries, secondSeries ), List.of( thirdSeries, fourthSeries ) );

        CrossPairs<Integer, Integer> expected = CrossPairs.of( List.of( firstSeries ), List.of( thirdSeries ) );

        assertEquals( expected, actual );
    }

    @Test
    public void testCrossPairTimeSeriesWithNoEqualReferenceTimesOrValidTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                               .addEvent( first )
                                                               .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ) );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );


        CrossPairs<Integer, Integer> expected =
                CrossPairs.of( List.of(), List.of() );

        assertEquals( expected, actual );
    }

    @Test
    public void testCrossPairTwoTimeSeriesWithEqualReferenceTimesAndNoEqualValidTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                               .addEvent( first )
                                                               .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );


        CrossPairs<Integer, Integer> expected =
                CrossPairs.of( List.of(), List.of() );

        assertEquals( expected, actual );
    }

    @Test
    public void testCrossPairTimeSeriesWithNoEqualReferenceTimesAndSomeEqualValidTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                               .addEvent( first )
                                                               .build();

        Event<Pair<Integer, Integer>> second = Event.of( FIRST, Pair.of( 2, 2 ) );
        Event<Pair<Integer, Integer>> third = Event.of( SECOND, Pair.of( 3, 3 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ) );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .addEvent( third )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );

        TimeSeries<Pair<Integer, Integer>> thirdSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .build();

        CrossPairs<Integer, Integer> expected =
                CrossPairs.of( List.of( firstSeries ), List.of( thirdSeries ) );

        assertEquals( expected, actual );
    }

    @Test
    public void testCrossPairTimeSeriesWithNoEqualReferenceTimesAndSomeEqualValidTimesWhenExactMatching()
    {
        TimeSeriesCrossPairer<Integer, Integer> crossPairerExact = TimeSeriesCrossPairer.of( MatchMode.EXACT );

        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                               .addEvent( first )
                                                               .build();

        Event<Pair<Integer, Integer>> second = Event.of( FIRST, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ) );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                crossPairerExact.apply( List.of( firstSeries ), List.of( secondSeries ) );

        CrossPairs<Integer, Integer> expected =
                CrossPairs.of( List.of(), List.of() );

        assertEquals( expected, actual );
    }

    @Test
    public void testCrossPairTwoTimeSeriesWithNoReferenceTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap() );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                               .addEvent( first )
                                                               .build();

        Event<Pair<Integer, Integer>> second = Event.of( FIRST, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata = TimeSeriesMetadata.of( Collections.emptyMap() );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );

        CrossPairs<Integer, Integer> expected =
                CrossPairs.of( List.of( firstSeries ), List.of( secondSeries ) );

        assertEquals( expected, actual );
    }

    @Test
    public void testCrossPairTimeSeriesWithSomeNearbyReferenceTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ) );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                               .addEvent( first )
                                                               .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ) );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                               .addEvent( second )
                                                               .build();

        Event<Pair<Integer, Integer>> third = Event.of( FIRST, Pair.of( 3, 3 ) );

        TimeSeriesMetadata thirdMetadata = firstMetadata;

        TimeSeries<Pair<Integer, Integer>> thirdSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( thirdMetadata )
                                                               .addEvent( third )
                                                               .build();

        Event<Pair<Integer, Integer>> fourth = Event.of( FOURTH, Pair.of( 4, 4 ) );

        TimeSeriesMetadata fourthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 SECOND ) );

        TimeSeries<Pair<Integer, Integer>> fourthSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( fourthMetadata )
                                                               .addEvent( fourth )
                                                               .build();

        Event<Pair<Integer, Integer>> fifth = Event.of( SECOND, Pair.of( 5, 5 ) );

        TimeSeriesMetadata fifthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ) );

        TimeSeries<Pair<Integer, Integer>> fifthSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( fifthMetadata )
                                                               .addEvent( fifth )
                                                               .build();


        Event<Pair<Integer, Integer>> sixth = Event.of( FIRST, Pair.of( 6, 6 ) );

        TimeSeriesMetadata sixthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 THIRD ) );

        TimeSeries<Pair<Integer, Integer>> sixthSeries =
                new TimeSeriesBuilder<Pair<Integer, Integer>>().setMetadata( sixthMetadata )
                                                               .addEvent( sixth )
                                                               .build();

        CrossPairs<Integer, Integer> actual =
                this.instance.apply( List.of( firstSeries, secondSeries ),
                                     List.of( thirdSeries, fourthSeries, fifthSeries, sixthSeries ) );

        CrossPairs<Integer, Integer> expected =
                CrossPairs.of( List.of( firstSeries, secondSeries ), List.of( thirdSeries, fifthSeries ) );

        assertEquals( expected, actual );
    }


}

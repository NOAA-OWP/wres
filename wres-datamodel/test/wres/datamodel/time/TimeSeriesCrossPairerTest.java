package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.yaml.components.CrossPairMethod;
import wres.datamodel.pools.pairs.CrossPairs;
import wres.datamodel.pools.pairs.PairingException;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries.Builder;

import wres.statistics.MessageFactory;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link Event}.
 *
 * @author James Brown
 */
class TimeSeriesCrossPairerTest
{
    private static final String KG_H = "kg/h";
    private static final Feature GEORGIA = Feature.of(
            MessageFactory.getGeometry( "Georgia" ) );
    private static final String CHICKENS = "Chickens";
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

    private TimeSeriesCrossPairer<Pair<Integer, Integer>, Pair<Integer, Integer>> instance;

    @BeforeEach
    void runBeforeEachTest()
    {
        this.instance = TimeSeriesCrossPairer.of();
    }

    @Test
    void testCrossPairTwoTimeSeriesWithEqualReferenceTimesThatEachAppearTwice()
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
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                     .addEvent( first )
                                                     .addEvent( second )
                                                     .addEvent( third )
                                                     .addEvent( fourth )
                                                     .build();

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                     .addEvent( sixth )
                                                     .addEvent( seventh )
                                                     .addEvent( eighth )
                                                     .addEvent( fifth )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                this.instance.apply( List.of( firstSeries, firstSeries ), List.of( secondSeries, secondSeries ) );

        TimeSeries<Pair<Integer, Integer>> expectedSeriesMain =
                new Builder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                     .addEvent( first )
                                                     .addEvent( second )
                                                     .addEvent( third )
                                                     .build();

        TimeSeries<Pair<Integer, Integer>> expectedSeriesBase =
                new Builder<Pair<Integer, Integer>>().setMetadata( metadata )
                                                     .addEvent( sixth )
                                                     .addEvent( seventh )
                                                     .addEvent( eighth )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of( expectedSeriesMain, expectedSeriesMain ),
                               List.of( expectedSeriesBase, expectedSeriesBase ) );

        assertEquals( expected, actual );

    }

    @Test
    void testCrossPairTimeSeriesWithSomeEqualReferenceTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        Event<Pair<Integer, Integer>> third = Event.of( FIRST, Pair.of( 3, 3 ) );

        TimeSeries<Pair<Integer, Integer>> thirdSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( third )
                                                     .build();

        Event<Pair<Integer, Integer>> fourth = Event.of( FOURTH, Pair.of( 4, 4 ) );

        TimeSeriesMetadata fourthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 SECOND ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> fourthSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( fourthMetadata )
                                                     .addEvent( fourth )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                this.instance.apply( List.of( firstSeries, secondSeries ), List.of( thirdSeries, fourthSeries ) );

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of( firstSeries ), List.of( thirdSeries ) );

        assertEquals( expected, actual );
    }

    @Test
    void testCrossPairTimeSeriesWithNoEqualReferenceTimesOrValidTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );


        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of(), List.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testCrossPairTwoTimeSeriesWithEqualReferenceTimesAndNoEqualValidTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );


        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of(), List.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testCrossPairTimeSeriesWithNoEqualReferenceTimesAndSomeEqualValidTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( FIRST, Pair.of( 2, 2 ) );
        Event<Pair<Integer, Integer>> third = Event.of( SECOND, Pair.of( 3, 3 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .addEvent( third )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );

        TimeSeries<Pair<Integer, Integer>> thirdSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of( firstSeries ), List.of( thirdSeries ) );

        assertEquals( expected, actual );
    }

    @Test
    void testCrossPairTimeSeriesWithNoEqualReferenceTimesAndSomeEqualValidTimesWhenExactMatching()
    {
        TimeSeriesCrossPairer<Pair<Integer, Integer>, Pair<Integer, Integer>> crossPairerExact =
                TimeSeriesCrossPairer.of( CrossPairMethod.EXACT );

        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( FIRST, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                crossPairerExact.apply( List.of( firstSeries ), List.of( secondSeries ) );

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of(), List.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testCrossPairTwoTimeSeriesWithNoReferenceTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap(),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( FIRST, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata = TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                   TimeScaleOuter.of(),
                                                                   CHICKENS,
                                                                   GEORGIA,
                                                                   KG_H );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                this.instance.apply( List.of( firstSeries ), List.of( secondSeries ) );

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of( firstSeries ), List.of( secondSeries ) );

        assertEquals( expected, actual );
    }

    @Test
    void testCrossPairTimeSeriesWithSomeNearbyReferenceTimes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( SECOND, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 FIRST ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        Event<Pair<Integer, Integer>> third = Event.of( FIRST, Pair.of( 3, 3 ) );

        TimeSeries<Pair<Integer, Integer>> thirdSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( third )
                                                     .build();

        Event<Pair<Integer, Integer>> fourth = Event.of( FOURTH, Pair.of( 4, 4 ) );

        TimeSeriesMetadata fourthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 SECOND ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );

        TimeSeries<Pair<Integer, Integer>> fourthSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( fourthMetadata )
                                                     .addEvent( fourth )
                                                     .build();

        Event<Pair<Integer, Integer>> fifth = Event.of( SECOND, Pair.of( 5, 5 ) );


        Instant nearToFirst = Instant.parse( "2123-12-01T06:01:00Z" );

        TimeSeriesMetadata fifthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 nearToFirst ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );

        TimeSeries<Pair<Integer, Integer>> fifthSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( fifthMetadata )
                                                     .addEvent( fifth )
                                                     .build();


        Event<Pair<Integer, Integer>> sixth = Event.of( FIRST, Pair.of( 6, 6 ) );

        TimeSeriesMetadata sixthMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 THIRD ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );

        TimeSeries<Pair<Integer, Integer>> sixthSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( sixthMetadata )
                                                     .addEvent( sixth )
                                                     .build();

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> actual =
                this.instance.apply( List.of( firstSeries, secondSeries ),
                                     List.of( thirdSeries, fourthSeries, fifthSeries, sixthSeries ) );

        CrossPairs<Pair<Integer, Integer>, Pair<Integer, Integer>> expected =
                CrossPairs.of( List.of( firstSeries, secondSeries ), List.of( thirdSeries, fifthSeries ) );

        assertEquals( expected, actual );
    }

    @Test
    void testCrossPairTimeSeriesWithNoEqualReferenceTimeTypes()
    {
        Event<Pair<Integer, Integer>> first = Event.of( FIRST, Pair.of( 1, 1 ) );

        TimeSeriesMetadata firstMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.T0,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );

        TimeSeries<Pair<Integer, Integer>> firstSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( firstMetadata )
                                                     .addEvent( first )
                                                     .build();

        Event<Pair<Integer, Integer>> second = Event.of( FIRST, Pair.of( 2, 2 ) );

        TimeSeriesMetadata secondMetadata =
                TimeSeriesMetadata.of( Collections.singletonMap( ReferenceTimeType.ANALYSIS_START_TIME,
                                                                 ZEROTH ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );

        TimeSeries<Pair<Integer, Integer>> secondSeries =
                new Builder<Pair<Integer, Integer>>().setMetadata( secondMetadata )
                                                     .addEvent( second )
                                                     .build();

        List<TimeSeries<Pair<Integer, Integer>>> firstThrow = List.of( firstSeries );
        List<TimeSeries<Pair<Integer, Integer>>> secondThrow = List.of( secondSeries );
        TimeSeriesCrossPairer<Pair<Integer, Integer>, Pair<Integer, Integer>> exact =
                TimeSeriesCrossPairer.of( CrossPairMethod.EXACT );
        PairingException exception = assertThrows( PairingException.class,
                                                   () -> exact.apply( firstThrow,
                                                                      secondThrow ) );

        // TODO, make an exception specific to the situation, assert that
        // the exception type is thrown, skip attempting to match message text.
        assertTrue( exception.getMessage()
                             .contains( "no commonly typed reference times" ) );
    }

    @Test
    void testCrossPairProducesSymmetricallyShapedPairs()
    {
        // #126644
        TimeSeriesMetadata m1 =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-12T01:15:00Z" ) ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeriesMetadata m2 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-12T15:39:00Z" ) ) )
                .build();
        TimeSeriesMetadata m3 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-13T14:40:00Z" ) ) )
                .build();
        TimeSeriesMetadata m4 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-14T15:15:00Z" ) ) )
                .build();
        TimeSeriesMetadata m5 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-15T15:24:00Z" ) ) )
                .build();
        TimeSeriesMetadata m6 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-16T14:14:00Z" ) ) )
                .build();
        TimeSeriesMetadata m7 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T12:17:00Z" ) ) )
                .build();
        TimeSeriesMetadata m8 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T09:20:00Z" ) ) )
                .build();
        TimeSeriesMetadata m9 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-17T13:57:00Z" ) ) )
                .build();
        TimeSeriesMetadata m10 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T02:49:00Z" ) ) )
                .build();
        TimeSeriesMetadata m11 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T14:29:00Z" ) ) )
                .build();
        TimeSeriesMetadata m12 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T20:04:00Z" ) ) )
                .build();
        TimeSeriesMetadata m13 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T01:32:00Z" ) ) )
                .build();
        TimeSeriesMetadata m14 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T07:04:00Z" ) ) )
                .build();
        TimeSeriesMetadata m15 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T14:25:00Z" ) ) )
                .build();
        TimeSeriesMetadata m16 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T19:18:00Z" ) ) )
                .build();
        TimeSeriesMetadata m17 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-20T02:48:00Z" ) ) )
                .build();
        TimeSeriesMetadata m18 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-21T01:25:00Z" ) ) )
                .build();
        TimeSeriesMetadata m19 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-20T13:44:00Z" ) ) )
                .build();
        TimeSeriesMetadata m20 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-22T14:45:00Z" ) ) )
                .build();
        TimeSeriesMetadata m21 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-21T14:40:00Z" ) ) )
                .build();

        TimeSeries<Double> f1 = new Builder<Double>().setMetadata( m1 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-12T06:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f2 = new Builder<Double>().setMetadata( m2 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-12T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f3 = new Builder<Double>().setMetadata( m3 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-13T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f4 = new Builder<Double>().setMetadata( m4 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-14T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f5 = new Builder<Double>().setMetadata( m5 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-15T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f6 = new Builder<Double>().setMetadata( m6 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-16T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f7 = new Builder<Double>().setMetadata( m7 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-18T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f8 = new Builder<Double>().setMetadata( m8 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-18T12:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f9 = new Builder<Double>().setMetadata( m9 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-17T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> f10 = new Builder<Double>().setMetadata( m10 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-18T06:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f11 = new Builder<Double>().setMetadata( m11 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-18T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f12 = new Builder<Double>().setMetadata( m12 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-19T00:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f13 = new Builder<Double>().setMetadata( m13 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-19T06:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f14 = new Builder<Double>().setMetadata( m14 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-19T12:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f15 = new Builder<Double>().setMetadata( m15 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-19T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f16 = new Builder<Double>().setMetadata( m16 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-20T00:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f17 = new Builder<Double>().setMetadata( m17 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-20T06:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f18 = new Builder<Double>().setMetadata( m18 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-21T06:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f19 = new Builder<Double>().setMetadata( m19 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-20T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f20 = new Builder<Double>().setMetadata( m20 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-22T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> f21 = new Builder<Double>().setMetadata( m21 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-21T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();

        TimeSeriesMetadata p1 =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-13T12:00:00Z" ) ),
                                       TimeScaleOuter.of(),
                                       CHICKENS,
                                       GEORGIA,
                                       KG_H );
        TimeSeriesMetadata p2 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-16T12:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p3 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T12:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p4 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T06:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p5 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-17T12:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p6 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T00:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p7 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-18T18:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p8 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T00:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p9 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T06:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p10 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T12:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p11 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-19T18:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p12 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-20T00:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p13 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-21T00:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p14 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-20T12:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p15 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-22T12:00:00Z" ) ) )
                .build();
        TimeSeriesMetadata p16 = new TimeSeriesMetadata.Builder( m1 )
                .setReferenceTimes( Map.of( ReferenceTimeType.T0, Instant.parse( "2023-12-21T12:00:00Z" ) ) )
                .build();

        TimeSeries<Double> g1 = new Builder<Double>().setMetadata( p1 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-13T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g2 = new Builder<Double>().setMetadata( p2 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-16T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g3 = new Builder<Double>().setMetadata( p3 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-18T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g4 = new Builder<Double>().setMetadata( p4 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-18T12:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g5 = new Builder<Double>().setMetadata( p5 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-17T18:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g6 = new Builder<Double>().setMetadata( p6 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-18T06:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g7 = new Builder<Double>().setMetadata( p7 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-19T00:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g8 = new Builder<Double>().setMetadata( p8 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-19T06:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g9 = new Builder<Double>().setMetadata( p9 )
                                                     .addEvent( Event.of( Instant.parse( "2023-12-19T12:00:00Z" ),
                                                                          1570.0 ) )
                                                     .build();
        TimeSeries<Double> g10 = new Builder<Double>().setMetadata( p10 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-19T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> g11 = new Builder<Double>().setMetadata( p11 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-20T00:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> g12 = new Builder<Double>().setMetadata( p12 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-20T06:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> g13 = new Builder<Double>().setMetadata( p13 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-21T06:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> g14 = new Builder<Double>().setMetadata( p14 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-20T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> g15 = new Builder<Double>().setMetadata( p15 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-22T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();
        TimeSeries<Double> g16 = new Builder<Double>().setMetadata( p16 )
                                                      .addEvent( Event.of( Instant.parse( "2023-12-21T18:00:00Z" ),
                                                                           1570.0 ) )
                                                      .build();

        List<TimeSeries<Double>> firstList = List.of( f1,
                                                      f2,
                                                      f3,
                                                      f4,
                                                      f5,
                                                      f6,
                                                      f7,
                                                      f8,
                                                      f9,
                                                      f10,
                                                      f11,
                                                      f12,
                                                      f13,
                                                      f14,
                                                      f15,
                                                      f16,
                                                      f17,
                                                      f18,
                                                      f19,
                                                      f20,
                                                      f21 );
        List<TimeSeries<Double>> secondList =
                List.of( g1, g2, g3, g4, g5, g6, g7, g8, g9, g10, g11, g12, g13, g14, g15, g16 );

        TimeSeriesCrossPairer<Double, Double> fuzzy = TimeSeriesCrossPairer.of( CrossPairMethod.FUZZY );

        CrossPairs<Double, Double> cross = fuzzy.apply( secondList, firstList );

        assertEquals( cross.getFirstPairs()
                           .size(), cross.getSecondPairs()
                                         .size() );
    }

}

package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.sampledata.pairs.PairingException;
import wres.datamodel.scale.TimeScale;

/**
 * Tests the {@link TimeSeriesPairerByExactTime}
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesPairerByExactTimeTest
{

    private static final Instant T2039_01_12T19_00_00Z = Instant.parse( "2039-01-12T19:00:00Z" );
    private static final Instant T2039_01_12T18_00_00Z = Instant.parse( "2039-01-12T18:00:00Z" );
    private static final Instant T2039_01_12T07_00_00Z = Instant.parse( "2039-01-12T07:00:00Z" );
    private static final Instant T2039_01_12T11_00_00Z = Instant.parse( "2039-01-12T11:00:00Z" );
    private static final Instant T2039_01_12T10_00_00Z = Instant.parse( "2039-01-12T10:00:00Z" );
    private static final Instant T2039_01_12T08_00_00Z = Instant.parse( "2039-01-12T08:00:00Z" );
    private static final Instant T2039_01_12T06_00_00Z = Instant.parse( "2039-01-12T06:00:00Z" );
    private static final Instant T2039_01_12T03_00_00Z = Instant.parse( "2039-01-12T03:00:00Z" );
    private static final Instant FIRST_TIME = Instant.parse( "2039-01-12T00:00:00Z" );
    private static final String SAUCE = "sauce";
    private static final String APPLE = "apple";

    @Test
    public void testPairObservationsCreatesFivePairs()
    {
        // Create a left series
        SortedSet<Event<String>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( FIRST_TIME, APPLE ) );
        leftEvents.add( Event.of( T2039_01_12T03_00_00Z, "banana" ) );
        leftEvents.add( Event.of( T2039_01_12T07_00_00Z, "grapefruit" ) );
        leftEvents.add( Event.of( T2039_01_12T08_00_00Z, "pear" ) );
        leftEvents.add( Event.of( T2039_01_12T10_00_00Z, "tangerine" ) );
        leftEvents.add( Event.of( T2039_01_12T11_00_00Z, "orange" ) );
        leftEvents.add( Event.of( T2039_01_12T18_00_00Z, "guava" ) );

        TimeSeries<String> left = TimeSeries.of( leftEvents );

        SortedSet<Event<String>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( FIRST_TIME, SAUCE ) );
        rightEvents.add( Event.of( T2039_01_12T03_00_00Z, "puree" ) );
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, "sorbet" ) );
        rightEvents.add( Event.of( T2039_01_12T08_00_00Z, "halfs" ) );
        rightEvents.add( Event.of( T2039_01_12T10_00_00Z, "spritzer" ) );
        rightEvents.add( Event.of( T2039_01_12T11_00_00Z, "juice" ) );
        rightEvents.add( Event.of( T2039_01_12T19_00_00Z, "chunks" ) );

        TimeSeries<String> right = TimeSeries.of( rightEvents );

        TimeSeriesPairer<String, String> pairer = TimeSeriesPairerByExactTime.of();

        // Create the actual pairs
        TimeSeries<Pair<String, String>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        SortedSet<Event<Pair<String, String>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( FIRST_TIME, Pair.of( APPLE, SAUCE ) ) );
        expectedEvents.add( Event.of( T2039_01_12T03_00_00Z, Pair.of( "banana", "puree" ) ) );
        expectedEvents.add( Event.of( T2039_01_12T08_00_00Z, Pair.of( "pear", "halfs" ) ) );
        expectedEvents.add( Event.of( T2039_01_12T10_00_00Z, Pair.of( "tangerine", "spritzer" ) ) );
        expectedEvents.add( Event.of( T2039_01_12T11_00_00Z, Pair.of( "orange", "juice" ) ) );

        TimeSeries<Pair<String, String>> expectedPairs = TimeSeries.of( expectedEvents );

        assertEquals( 5, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairObservationsCreatesZeroPairs()
    {
        // Create a left series
        SortedSet<Event<String>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( FIRST_TIME, APPLE ) );

        TimeSeries<String> left = TimeSeries.of( leftEvents );

        SortedSet<Event<String>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, SAUCE ) );

        TimeSeries<String> right = TimeSeries.of( rightEvents );

        TimeSeriesPairer<String, String> pairer = TimeSeriesPairerByExactTime.of();

        // Create the actual pairs
        TimeSeries<Pair<String, String>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        TimeSeries<Pair<String, String>> expectedPairs =
                TimeSeries.of( T2039_01_12T06_00_00Z, new TreeSet<>() );

        assertEquals( 0, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairObservationsAndSingleValuedForecastsCreatesThreePairs()
    {
        // Create a left series
        SortedSet<Event<Double>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( FIRST_TIME, 1.0 ) );
        leftEvents.add( Event.of( T2039_01_12T03_00_00Z, 3.0 ) );
        leftEvents.add( Event.of( T2039_01_12T07_00_00Z, 7.0 ) );
        leftEvents.add( Event.of( T2039_01_12T08_00_00Z, 15.0 ) );
        leftEvents.add( Event.of( T2039_01_12T10_00_00Z, 79.0 ) );
        leftEvents.add( Event.of( T2039_01_12T11_00_00Z, 80.0 ) );
        leftEvents.add( Event.of( T2039_01_12T18_00_00Z, 93.0 ) );

        TimeSeries<Double> left = TimeSeries.of( leftEvents );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( FIRST_TIME, 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T02:00:00Z" ), 3.0 ) );
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, 7.0 ) );
        rightEvents.add( Event.of( T2039_01_12T08_00_00Z, 15.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T09:00:00Z" ), 79.0 ) );
        rightEvents.add( Event.of( T2039_01_12T11_00_00Z, 80.0 ) );
        rightEvents.add( Event.of( T2039_01_12T19_00_00Z, 93.0 ) );

        Instant referenceTime = Instant.parse( "2039-01-01T00:00:00Z" );

        TimeSeries<Double> right = TimeSeries.of( referenceTime, rightEvents );

        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();

        // Create the actual pairs
        TimeSeries<Pair<Double, Double>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( FIRST_TIME, Pair.of( 1.0, 1.0 ) ) );
        expectedEvents.add( Event.of( T2039_01_12T08_00_00Z, Pair.of( 15.0, 15.0 ) ) );
        expectedEvents.add( Event.of( T2039_01_12T11_00_00Z, Pair.of( 80.0, 80.0 ) ) );

        TimeSeries<Pair<Double, Double>> expectedPairs = TimeSeries.of( referenceTime, expectedEvents );

        assertEquals( 3, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairObservationsAndSingleValuedForecastsCreatesOnePairAndDisregardsTwo()
    {
        // Tests for pairing with inadmissible values

        // Create a left series
        SortedSet<Event<Double>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( FIRST_TIME, Double.POSITIVE_INFINITY ) );
        leftEvents.add( Event.of( T2039_01_12T03_00_00Z, 3.0 ) );
        leftEvents.add( Event.of( T2039_01_12T07_00_00Z, 7.0 ) );
        leftEvents.add( Event.of( T2039_01_12T08_00_00Z, 15.0 ) );
        leftEvents.add( Event.of( T2039_01_12T10_00_00Z, 79.0 ) );
        leftEvents.add( Event.of( T2039_01_12T11_00_00Z, 80.0 ) );
        leftEvents.add( Event.of( T2039_01_12T18_00_00Z, 93.0 ) );

        TimeSeries<Double> left = TimeSeries.of( leftEvents );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( FIRST_TIME, 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T02:00:00Z" ), 3.0 ) );
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, 7.0 ) );
        rightEvents.add( Event.of( T2039_01_12T08_00_00Z, Double.NaN ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T09:00:00Z" ), 79.0 ) );
        rightEvents.add( Event.of( T2039_01_12T11_00_00Z, 80.0 ) );
        rightEvents.add( Event.of( T2039_01_12T19_00_00Z, 93.0 ) );

        Instant referenceTime = Instant.parse( "2039-01-01T00:00:00Z" );

        TimeSeries<Double> right = TimeSeries.of( referenceTime, rightEvents );

        // Do not admit values unless they are finite
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of( Double::isFinite, Double::isFinite );

        // Create the actual pairs
        TimeSeries<Pair<Double, Double>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( T2039_01_12T11_00_00Z, Pair.of( 80.0, 80.0 ) ) );

        TimeSeries<Pair<Double, Double>> expectedPairs = TimeSeries.of( referenceTime, expectedEvents );

        assertEquals( 1, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairThrowsExceptionWhenNullLeftIsNull()
    {
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();
        TimeSeries<Double> series =
                TimeSeries.of( new TreeSet<>( Collections.singleton( Event.of( Instant.now(), 1.0 ) ) ) );

        NullPointerException exception = assertThrows( NullPointerException.class,
                                                       () -> pairer.pair( null, series ) );

        assertEquals( "Cannot pair a left time-series that is null.", exception.getMessage() );
    }

    @Test
    public void testPairThrowsExceptionWhenRightIsNull()
    {
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();
        TimeSeries<Double> series =
                TimeSeries.of( new TreeSet<>( Collections.singleton( Event.of( Instant.now(), 1.0 ) ) ) );

        NullPointerException exception = assertThrows( NullPointerException.class,
                                                       () -> pairer.pair( series, null ) );

        assertEquals( "Cannot pair a right time-series that is null.", exception.getMessage() );
    }

    @Test
    public void testPairThrowsExceptionWhenLeftAndRightHaveDifferentTimeScales()
    {
        TimeSeriesPairer<Object, Object> pairer = TimeSeriesPairerByExactTime.of();

        TimeSeries<Object> seriesOne =
                new TimeSeries.TimeSeriesBuilder<>().setTimeScale( TimeScale.of() ).build();

        TimeSeries<Object> seriesTwo =
                new TimeSeries.TimeSeriesBuilder<>().setTimeScale( TimeScale.of( Duration.ofHours( 1 ) ) ).build();


        TimeSeries.of( new TreeSet<>( Collections.singleton( Event.of( Instant.now(), 1.0 ) ) ) );

        PairingException exception = assertThrows( PairingException.class,
                                                   () -> pairer.pair( seriesOne, seriesTwo ) );

        assertEquals( "Cannot pair two datasets with different time scales. The left time-series has a time-scale of "
                      + "'[INSTANTANEOUS]' and the right time-series has a time-scale of '[PT1H,UNKNOWN]'.",
                      exception.getMessage() );
    }

}

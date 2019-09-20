package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Instant;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Tests the {@link CrispPairerByValidTime}
 * 
 * @author james.brown@hydrosolved.com
 */

public class CrispPairerByValidTimeTest
{

    private static final String SIXTH_TIME = "2039-01-12T11:00:00Z";
    private static final String FIFTH_TIME = "2039-01-12T10:00:00Z";
    private static final String FOURTH_TIME = "2039-01-12T08:00:00Z";
    private static final String THIRD_TIME = "2039-01-12T06:00:00Z";
    private static final String SECOND_TIME = "2039-01-12T03:00:00Z";
    private static final String FIRST_TIME = "2039-01-12T00:00:00Z";
    private static final String SAUCE = "sauce";
    private static final String APPLE = "apple";

    @Test
    public void testPairObservationsCreatesFivePairs()
    {
        // Create a left series
        SortedSet<Event<String>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( Instant.parse( FIRST_TIME ), APPLE ) );
        leftEvents.add( Event.of( Instant.parse( SECOND_TIME ), "banana" ) );
        leftEvents.add( Event.of( Instant.parse( "2039-01-12T07:00:00Z" ), "grapefruit" ) );
        leftEvents.add( Event.of( Instant.parse( FOURTH_TIME ), "pear" ) );
        leftEvents.add( Event.of( Instant.parse( FIFTH_TIME ), "tangerine" ) );
        leftEvents.add( Event.of( Instant.parse( SIXTH_TIME ), "orange" ) );
        leftEvents.add( Event.of( Instant.parse( "2039-01-12T18:00:00Z" ), "guava" ) );

        TimeSeries<String> left = TimeSeries.of( leftEvents );

        SortedSet<Event<String>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( Instant.parse( FIRST_TIME ), SAUCE ) );
        rightEvents.add( Event.of( Instant.parse( SECOND_TIME ), "puree" ) );
        rightEvents.add( Event.of( Instant.parse( THIRD_TIME ), "sorbet" ) );
        rightEvents.add( Event.of( Instant.parse( FOURTH_TIME ), "halfs" ) );
        rightEvents.add( Event.of( Instant.parse( FIFTH_TIME ), "spritzer" ) );
        rightEvents.add( Event.of( Instant.parse( SIXTH_TIME ), "juice" ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T19:00:00Z" ), "chunks" ) );

        TimeSeries<String> right = TimeSeries.of( rightEvents );

        TimeSeriesPairer<String, String> pairer = CrispPairerByValidTime.of();

        // Create the actual pairs
        TimeSeries<Pair<String, String>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        SortedSet<Event<Pair<String, String>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( Instant.parse( FIRST_TIME ), Pair.of( APPLE, SAUCE ) ) );
        expectedEvents.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( "banana", "puree" ) ) );
        expectedEvents.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( "pear", "halfs" ) ) );
        expectedEvents.add( Event.of( Instant.parse( FIFTH_TIME ), Pair.of( "tangerine", "spritzer" ) ) );
        expectedEvents.add( Event.of( Instant.parse( SIXTH_TIME ), Pair.of( "orange", "juice" ) ) );

        TimeSeries<Pair<String, String>> expectedPairs = TimeSeries.of( expectedEvents );

        assertEquals( 5, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairObservationsCreatesZeroPairs()
    {
        // Create a left series
        SortedSet<Event<String>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( Instant.parse( FIRST_TIME ), APPLE ) );

        TimeSeries<String> left = TimeSeries.of( leftEvents );

        SortedSet<Event<String>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( Instant.parse( THIRD_TIME ), SAUCE ) );

        TimeSeries<String> right = TimeSeries.of( rightEvents );

        TimeSeriesPairer<String, String> pairer = CrispPairerByValidTime.of();

        // Create the actual pairs
        TimeSeries<Pair<String, String>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        TimeSeries<Pair<String, String>> expectedPairs =
                TimeSeries.of( Instant.parse( THIRD_TIME ), new TreeSet<>() );

        assertEquals( 0, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairObservationsAndSingleValuedForecastsCreatesThreePairs()
    {
        // Create a left series
        SortedSet<Event<Double>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( Instant.parse( FIRST_TIME ), 1.0 ) );
        leftEvents.add( Event.of( Instant.parse( SECOND_TIME ), 3.0 ) );
        leftEvents.add( Event.of( Instant.parse( "2039-01-12T07:00:00Z" ), 7.0 ) );
        leftEvents.add( Event.of( Instant.parse( FOURTH_TIME ), 15.0 ) );
        leftEvents.add( Event.of( Instant.parse( FIFTH_TIME ), 79.0 ) );
        leftEvents.add( Event.of( Instant.parse( SIXTH_TIME ), 80.0 ) );
        leftEvents.add( Event.of( Instant.parse( "2039-01-12T18:00:00Z" ), 93.0 ) );

        TimeSeries<Double> left = TimeSeries.of( leftEvents );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( Instant.parse( FIRST_TIME ), 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T02:00:00Z" ), 3.0 ) );
        rightEvents.add( Event.of( Instant.parse( THIRD_TIME ), 7.0 ) );
        rightEvents.add( Event.of( Instant.parse( FOURTH_TIME ), 15.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T09:00:00Z" ), 79.0 ) );
        rightEvents.add( Event.of( Instant.parse( SIXTH_TIME ), 80.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T19:00:00Z" ), 93.0 ) );

        Instant referenceTime = Instant.parse( "2039-01-01T00:00:00Z" );

        TimeSeries<Double> right = TimeSeries.of( referenceTime, rightEvents );

        TimeSeriesPairer<Double, Double> pairer = CrispPairerByValidTime.of();

        // Create the actual pairs
        TimeSeries<Pair<Double, Double>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( Instant.parse( FIRST_TIME ), Pair.of( 1.0, 1.0 ) ) );
        expectedEvents.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 15.0, 15.0 ) ) );
        expectedEvents.add( Event.of( Instant.parse( SIXTH_TIME ), Pair.of( 80.0, 80.0 ) ) );

        TimeSeries<Pair<Double, Double>> expectedPairs = TimeSeries.of( referenceTime, expectedEvents );

        assertEquals( 3, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairThrowsExceptionWhenNullLeftIsNull()
    {
        TimeSeriesPairer<Double, Double> pairer = CrispPairerByValidTime.of();
        TimeSeries<Double> series =
                TimeSeries.of( new TreeSet<>( Collections.singleton( Event.of( Instant.now(), 1.0 ) ) ) );

        NullPointerException exception = assertThrows( NullPointerException.class,
                                                       () -> pairer.pair( null, series ) );

        assertEquals( "Cannot pair a left time-series that is null.", exception.getMessage() );
    }

    @Test
    public void testPairThrowsExceptionWhenRightIsNull()
    {
        TimeSeriesPairer<Double, Double> pairer = CrispPairerByValidTime.of();
        TimeSeries<Double> series =
                TimeSeries.of( new TreeSet<>( Collections.singleton( Event.of( Instant.now(), 1.0 ) ) ) );

        NullPointerException exception = assertThrows( NullPointerException.class,
                                                       () -> pairer.pair( series, null ) );

        assertEquals( "Cannot pair a right time-series that is null.", exception.getMessage() );
    }

}

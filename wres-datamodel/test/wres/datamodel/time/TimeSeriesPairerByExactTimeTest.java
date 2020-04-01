package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.sampledata.pairs.PairingException;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Tests the {@link TimeSeriesPairerByExactTime}
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesPairerByExactTimeTest
{

    private static final Instant T2551_03_19T09_00_00Z = Instant.parse( "2551-03-19T09:00:00Z" );
    private static final Instant T2551_03_19T06_00_00Z = Instant.parse( "2551-03-19T06:00:00Z" );
    private static final Instant T2551_03_19T03_00_00Z = Instant.parse( "2551-03-19T03:00:00Z" );
    private static final Instant T2551_03_19T00_00_00Z = Instant.parse( "2551-03-19T00:00:00Z" );
    private static final Instant T2551_03_17T12_00_00Z = Instant.parse( "2551-03-17T12:00:00Z" );
    private static final Instant T2551_03_18T21_00_00Z = Instant.parse( "2551-03-18T21:00:00Z" );
    private static final Instant T2551_03_18T18_00_00Z = Instant.parse( "2551-03-18T18:00:00Z" );
    private static final Instant T2551_03_18T15_00_00Z = Instant.parse( "2551-03-18T15:00:00Z" );
    private static final Instant T2551_03_18T12_00_00Z = Instant.parse( "2551-03-18T12:00:00Z" );
    private static final Instant T2551_03_18T09_00_00Z = Instant.parse( "2551-03-18T09:00:00Z" );
    private static final Instant T2551_03_18T06_00_00Z = Instant.parse( "2551-03-18T06:00:00Z" );
    private static final Instant T2551_03_18T03_00_00Z = Instant.parse( "2551-03-18T03:00:00Z" );
    private static final Instant T2551_03_18T00_00_00Z = Instant.parse( "2551-03-18T00:00:00Z" );
    private static final Instant T2551_03_17T21_00_00Z = Instant.parse( "2551-03-17T21:00:00Z" );
    private static final Instant T2551_03_17T18_00_00Z = Instant.parse( "2551-03-17T18:00:00Z" );
    private static final Instant T2551_03_17T15_00_00Z = Instant.parse( "2551-03-17T15:00:00Z" );
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

    private static final String VARIABLE_NAME = "Fruit";
    private static final String FEATURE_NAME = "Tropics";
    private static final String UNIT = "kg/h";

    private static TimeSeriesMetadata getBoilerplateMetadata()
    {
        return TimeSeriesMetadata.of( Collections.emptyMap(),
                                      TimeScale.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0 )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      TimeScale.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

    private static TimeSeriesMetadata getBoilerplateMetadataWithTimeScale( Duration timeScale )
    {
        return TimeSeriesMetadata.of( Collections.emptyMap(),
                                      TimeScale.of( timeScale ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

    @Test
    public void testPairObservationsCreatesFivePairs()
    {
        // Create a left series
        TimeSeriesMetadata metadata = getBoilerplateMetadata();
        SortedSet<Event<String>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( FIRST_TIME, APPLE ) );
        leftEvents.add( Event.of( T2039_01_12T03_00_00Z, "banana" ) );
        leftEvents.add( Event.of( T2039_01_12T07_00_00Z, "grapefruit" ) );
        leftEvents.add( Event.of( T2039_01_12T08_00_00Z, "pear" ) );
        leftEvents.add( Event.of( T2039_01_12T10_00_00Z, "tangerine" ) );
        leftEvents.add( Event.of( T2039_01_12T11_00_00Z, "orange" ) );
        leftEvents.add( Event.of( T2039_01_12T18_00_00Z, "guava" ) );

        TimeSeries<String> left = TimeSeries.of( metadata, leftEvents );

        SortedSet<Event<String>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( FIRST_TIME, SAUCE ) );
        rightEvents.add( Event.of( T2039_01_12T03_00_00Z, "puree" ) );
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, "sorbet" ) );
        rightEvents.add( Event.of( T2039_01_12T08_00_00Z, "halfs" ) );
        rightEvents.add( Event.of( T2039_01_12T10_00_00Z, "spritzer" ) );
        rightEvents.add( Event.of( T2039_01_12T11_00_00Z, "juice" ) );
        rightEvents.add( Event.of( T2039_01_12T19_00_00Z, "chunks" ) );

        TimeSeries<String> right = TimeSeries.of( metadata, rightEvents );

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

        TimeSeries<Pair<String, String>> expectedPairs = TimeSeries.of( metadata,
                                                                        expectedEvents );

        assertEquals( 5, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairObservationsCreatesZeroPairs()
    {
        // Create a left series
        TimeSeriesMetadata metadata = getBoilerplateMetadata();
        SortedSet<Event<String>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( FIRST_TIME, APPLE ) );

        TimeSeries<String> left = TimeSeries.of( metadata, leftEvents );

        SortedSet<Event<String>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, SAUCE ) );

        TimeSeries<String> right = TimeSeries.of( metadata, rightEvents );

        TimeSeriesPairer<String, String> pairer = TimeSeriesPairerByExactTime.of();

        // Create the actual pairs
        TimeSeries<Pair<String, String>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        TimeSeries<Pair<String, String>> expectedPairs =
                TimeSeries.of( metadata );

        assertEquals( 0, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairObservationsAndSingleValuedForecastsCreatesThreePairs()
    {
        // Create a left series
        TimeSeriesMetadata leftMetadata = getBoilerplateMetadata();
        SortedSet<Event<Double>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( FIRST_TIME, 1.0 ) );
        leftEvents.add( Event.of( T2039_01_12T03_00_00Z, 3.0 ) );
        leftEvents.add( Event.of( T2039_01_12T07_00_00Z, 7.0 ) );
        leftEvents.add( Event.of( T2039_01_12T08_00_00Z, 15.0 ) );
        leftEvents.add( Event.of( T2039_01_12T10_00_00Z, 79.0 ) );
        leftEvents.add( Event.of( T2039_01_12T11_00_00Z, 80.0 ) );
        leftEvents.add( Event.of( T2039_01_12T18_00_00Z, 93.0 ) );

        TimeSeries<Double> left = TimeSeries.of( leftMetadata,
                                                 leftEvents );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( FIRST_TIME, 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T02:00:00Z" ), 3.0 ) );
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, 7.0 ) );
        rightEvents.add( Event.of( T2039_01_12T08_00_00Z, 15.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T09:00:00Z" ), 79.0 ) );
        rightEvents.add( Event.of( T2039_01_12T11_00_00Z, 80.0 ) );
        rightEvents.add( Event.of( T2039_01_12T19_00_00Z, 93.0 ) );

        Instant referenceTime = Instant.parse( "2039-01-01T00:00:00Z" );
        TimeSeriesMetadata rightMetadata = getBoilerplateMetadataWithT0( referenceTime );
        TimeSeries<Double> right = TimeSeries.of( rightMetadata, rightEvents );

        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();

        // Create the actual pairs
        TimeSeries<Pair<Double, Double>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( FIRST_TIME, Pair.of( 1.0, 1.0 ) ) );
        expectedEvents.add( Event.of( T2039_01_12T08_00_00Z, Pair.of( 15.0, 15.0 ) ) );
        expectedEvents.add( Event.of( T2039_01_12T11_00_00Z, Pair.of( 80.0, 80.0 ) ) );

        TimeSeries<Pair<Double, Double>> expectedPairs = TimeSeries.of( rightMetadata, expectedEvents );

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

        TimeSeries<Double> left = TimeSeries.of( getBoilerplateMetadata(),
                                                 leftEvents );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( FIRST_TIME, 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T02:00:00Z" ), 3.0 ) );
        rightEvents.add( Event.of( T2039_01_12T06_00_00Z, 7.0 ) );
        rightEvents.add( Event.of( T2039_01_12T08_00_00Z, Double.NaN ) );
        rightEvents.add( Event.of( Instant.parse( "2039-01-12T09:00:00Z" ), 79.0 ) );
        rightEvents.add( Event.of( T2039_01_12T11_00_00Z, 80.0 ) );
        rightEvents.add( Event.of( T2039_01_12T19_00_00Z, 93.0 ) );

        Instant referenceTime = Instant.parse( "2039-01-01T00:00:00Z" );
        TimeSeriesMetadata rightMetadata = getBoilerplateMetadataWithT0( referenceTime );
        TimeSeries<Double> right = TimeSeries.of( rightMetadata, rightEvents );

        // Do not admit values unless they are finite
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of( Double::isFinite, Double::isFinite );

        // Create the actual pairs
        TimeSeries<Pair<Double, Double>> actualPairs = pairer.pair( left, right );

        // Created the expected time-series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( T2039_01_12T11_00_00Z, Pair.of( 80.0, 80.0 ) ) );

        TimeSeries<Pair<Double, Double>> expectedPairs = TimeSeries.of( rightMetadata, expectedEvents );

        assertEquals( 1, expectedPairs.getEvents().size() );

        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testPairSingleValuedForecastsAndSingleValuedForecastsCreatesTwentyTwoPairs()
    {
        // Case described in #72042-12, based on system test scenario504

        // First time-series
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                        T2551_03_17T12_00_00Z ),
                                                                TimeScale.of( Duration.ofHours( 3 ) ),
                                                                "STREAMFLOW",
                                                                "FAKE2",
                                                                "CMS" );
        SortedSet<Event<Double>> firstEvents = new TreeSet<>();
        firstEvents.add( Event.of( T2551_03_17T15_00_00Z, 73.0 ) );
        firstEvents.add( Event.of( T2551_03_17T18_00_00Z, 79.0 ) );
        firstEvents.add( Event.of( T2551_03_17T21_00_00Z, 83.0 ) );
        firstEvents.add( Event.of( T2551_03_18T00_00_00Z, 89.0 ) );
        firstEvents.add( Event.of( T2551_03_18T03_00_00Z, 97.0 ) );
        firstEvents.add( Event.of( T2551_03_18T06_00_00Z, 101.0 ) );
        firstEvents.add( Event.of( T2551_03_18T09_00_00Z, 103.0 ) );
        firstEvents.add( Event.of( T2551_03_18T12_00_00Z, 107.0 ) );
        firstEvents.add( Event.of( T2551_03_18T15_00_00Z, 109.0 ) );
        firstEvents.add( Event.of( T2551_03_18T18_00_00Z, 113.0 ) );
        firstEvents.add( Event.of( T2551_03_18T21_00_00Z, 127.0 ) );

        TimeSeries<Double> first = TimeSeries.of( metadataOne, firstEvents );

        // Second time-series
        TimeSeriesMetadata metadataTwo = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                        T2551_03_18T00_00_00Z ),
                                                                TimeScale.of( Duration.ofHours( 3 ) ),
                                                                "STREAMFLOW",
                                                                "FAKE2",
                                                                "CMS" );
        SortedSet<Event<Double>> secondEvents = new TreeSet<>();
        secondEvents.add( Event.of( T2551_03_18T03_00_00Z, 131.0 ) );
        secondEvents.add( Event.of( T2551_03_18T06_00_00Z, 137.0 ) );
        secondEvents.add( Event.of( T2551_03_18T09_00_00Z, 139.0 ) );
        secondEvents.add( Event.of( T2551_03_18T12_00_00Z, 149.0 ) );
        secondEvents.add( Event.of( T2551_03_18T15_00_00Z, 191.0 ) );
        secondEvents.add( Event.of( T2551_03_18T18_00_00Z, 157.0 ) );
        secondEvents.add( Event.of( T2551_03_18T21_00_00Z, 163.0 ) );
        secondEvents.add( Event.of( T2551_03_19T00_00_00Z, 167.0 ) );
        secondEvents.add( Event.of( T2551_03_19T03_00_00Z, 173.0 ) );
        secondEvents.add( Event.of( T2551_03_19T06_00_00Z, 179.0 ) );
        secondEvents.add( Event.of( T2551_03_19T09_00_00Z, 181.0 ) );

        TimeSeries<Double> second = TimeSeries.of( metadataTwo, secondEvents );

        // Create the pairer
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of( Double::isFinite, Double::isFinite );

        // Intersect the matching time-series combinations first, then the non-matching combinations
        // First of four
        TimeSeries<Pair<Double, Double>> firstActual = pairer.pair( first, first );
        TimeSeries<Pair<Double, Double>> firstExpected =
                new TimeSeriesBuilder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_17T15_00_00Z,
                                                                                  Pair.of( 73.0, 73.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_17T18_00_00Z,
                                                                                  Pair.of( 79.0, 79.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_17T21_00_00Z,
                                                                                  Pair.of( 83.0, 83.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T00_00_00Z,
                                                                                  Pair.of( 89.0, 89.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T03_00_00Z,
                                                                                  Pair.of( 97.0, 97.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T06_00_00Z,
                                                                                  Pair.of( 101.0, 101.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T09_00_00Z,
                                                                                  Pair.of( 103.0, 103.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T12_00_00Z,
                                                                                  Pair.of( 107.0, 107.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T15_00_00Z,
                                                                                  Pair.of( 109.0, 109.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T18_00_00Z,
                                                                                  Pair.of( 113.0, 113.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T21_00_00Z,
                                                                                  Pair.of( 127.0, 127.0 ) ) )
                                                             .setMetadata( metadataOne )
                                                             .build();

        assertEquals( firstExpected, firstActual );

        // Second of four
        TimeSeries<Pair<Double, Double>> secondActual = pairer.pair( second, second );

        TimeSeries<Pair<Double, Double>> secondExpected =
                new TimeSeriesBuilder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_18T03_00_00Z,
                                                                                  Pair.of( 131.0, 131.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T06_00_00Z,
                                                                                  Pair.of( 137.0, 137.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T09_00_00Z,
                                                                                  Pair.of( 139.0, 139.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T12_00_00Z,
                                                                                  Pair.of( 149.0, 149.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T15_00_00Z,
                                                                                  Pair.of( 191.0, 191.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T18_00_00Z,
                                                                                  Pair.of( 157.0, 157.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T21_00_00Z,
                                                                                  Pair.of( 163.0, 163.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T00_00_00Z,
                                                                                  Pair.of( 167.0, 167.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T03_00_00Z,
                                                                                  Pair.of( 173.0, 173.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T06_00_00Z,
                                                                                  Pair.of( 179.0, 179.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T09_00_00Z,
                                                                                  Pair.of( 181.0, 181.0 ) ) )
                                                             .setMetadata( metadataTwo )
                                                             .build();

        assertEquals( secondExpected, secondActual );

        // Third of four
        TimeSeries<Pair<Double, Double>> thirdActual = pairer.pair( first, second );
        TimeSeries<Pair<Double, Double>> thirdExpected = TimeSeries.of( metadataTwo );

        assertEquals( thirdExpected, thirdActual );

        // Fourth of four
        TimeSeries<Pair<Double, Double>> fourthActual = pairer.pair( second, first );
        TimeSeries<Pair<Double, Double>> fourthExpected = TimeSeries.of( metadataOne );

        assertEquals( fourthExpected, fourthActual );
    }

    @Test
    public void testPairThrowsExceptionWhenNullLeftIsNull()
    {
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();
        TimeSeriesMetadata metadata = getBoilerplateMetadata();
        TimeSeries<Double> series =
                TimeSeries.of( metadata, new TreeSet<>( Collections.singleton( Event.of( Instant.now(), 1.0 ) ) ) );

        NullPointerException exception = assertThrows( NullPointerException.class,
                                                       () -> pairer.pair( null, series ) );

        assertEquals( "Cannot pair a left time-series that is null.", exception.getMessage() );
    }

    @Test
    public void testPairThrowsExceptionWhenRightIsNull()
    {
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();
        TimeSeriesMetadata metadata = getBoilerplateMetadata();
        TimeSeries<Double> series =
                TimeSeries.of( metadata,
                               new TreeSet<>( Collections.singleton( Event.of( Instant.now(), 1.0 ) ) ) );

        NullPointerException exception = assertThrows( NullPointerException.class,
                                                       () -> pairer.pair( series, null ) );

        assertEquals( "Cannot pair a right time-series that is null.", exception.getMessage() );
    }

    @Test
    public void testPairThrowsExceptionWhenLeftAndRightHaveDifferentTimeScales()
    {
        TimeSeriesPairer<Object, Object> pairer = TimeSeriesPairerByExactTime.of();

        TimeSeriesMetadata metadataOne = getBoilerplateMetadataWithTimeScale( Duration.ofMinutes( 1 ) );
        TimeSeries<Object> seriesOne =
                new TimeSeries.TimeSeriesBuilder<>().setMetadata( metadataOne ).build();

        TimeSeriesMetadata metadataTwo = getBoilerplateMetadataWithTimeScale( Duration.ofHours( 1 ) );
        TimeSeries<Object> seriesTwo =
                new TimeSeries.TimeSeriesBuilder<>().setMetadata( metadataTwo ).build();

        PairingException exception = assertThrows( PairingException.class,
                                                   () -> pairer.pair( seriesOne, seriesTwo ) );

        assertEquals( "Cannot pair two datasets with different time scales. The left time-series has a time-scale of "
                      + "'[INSTANTANEOUS]' and the right time-series has a time-scale of '[PT1H,UNKNOWN]'.",
                      exception.getMessage() );
    }

}

package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

import org.junit.Test;

import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindow;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Tests the {@link PoolOfPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class PoolOfPairsTest
{

    private static final String ZERO_ZEE = ":00:00Z";
    private static final String EIGHTH_TIME = "1985-01-03T01:00:00Z";
    private static final String SEVENTH_TIME = "1985-01-03T00:00:00Z";
    private static final String SIXTH_TIME = "1985-01-02T02:00:00Z";
    private static final String FIFTH_TIME = "1985-01-02T00:00:00Z";
    private static final String FOURTH_TIME = "1985-01-01T03:00:00Z";
    private static final String THIRD_TIME = "1985-01-01T02:00:00Z";
    private static final String SECOND_TIME = "1985-01-01T01:00:00Z";
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    @Test
    public void testGetBaselineData()
    {
        //Build a time-series with two basis times
        SortedSet<Event<Pair<Double, Double>>> values = new TreeSet<>();
        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        values.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 2.0 ) ) );
        values.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 3.0 ) ) );
        SampleMetadata meta = SampleMetadata.of();

        TimeSeries<Pair<Double, Double>> timeSeries = TimeSeries.of( basisTime,
                                                                     values );
        b.addTimeSeries( timeSeries ).setMetadata( meta );

        PoolOfPairs<Double, Double> expectedBaseline = b.build();

        assertNull( b.build().getBaselineData() );

        b.addTimeSeriesForBaseline( timeSeries ).setMetadataForBaseline( meta );

        PoolOfPairs<Double, Double> actualBaseline = b.build().getBaselineData();

        assertNotNull( actualBaseline );

        assertEquals( expectedBaseline, actualBaseline );
    }

    @Test
    public void testAddMultipleTimeSeriesWithSameReferenceTime()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();

        Instant basisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 3.0 ) ) );

        TimeSeries<Pair<Double, Double>> firstSeries = TimeSeries.of( basisTime,
                                                                      first );

        //Add the first time-series and then append a second and third
        PoolOfPairsBuilder<Double, Double> c = new PoolOfPairsBuilder<>();
        c.addTimeSeries( firstSeries );

        second.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 4.0, 4.0 ) ) );
        second.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 5.0, 5.0 ) ) );
        second.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 6.0, 6.0 ) ) );

        third.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 7.0, 7.0 ) ) );
        third.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 8.0, 8.0 ) ) );
        third.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 9.0, 9.0 ) ) );

        TimeSeries<Pair<Double, Double>> secondSeries = TimeSeries.of( basisTime,
                                                                       second );

        TimeSeries<Pair<Double, Double>> thirdSeries = TimeSeries.of( basisTime,
                                                                      third );

        SampleMetadata meta = SampleMetadata.of();

        c.addTimeSeries( secondSeries )
         .addTimeSeries( thirdSeries )
         .addTimeSeriesForBaseline( secondSeries )
         .addTimeSeriesForBaseline( thirdSeries )
         .setMetadata( meta )
         .setMetadataForBaseline( meta );

        PoolOfPairs<Double, Double> tsAppend = c.build();

        //Check dataset dimensions
        SortedSet<Duration> durations = new TreeSet<>();
        durations.add( Duration.ofHours( 1 ) );
        durations.add( Duration.ofHours( 2 ) );
        durations.add( Duration.ofHours( 3 ) );

        assertTrue( StreamSupport.stream( tsAppend.get().spliterator(),
                                          false )
                                 .count() == 3 );
        //Check dataset
        //Iterate and test
        double nextValue = 1.0;
        for ( TimeSeries<Pair<Double, Double>> nextSeries : tsAppend.get() )
        {
            for ( Event<Pair<Double, Double>> nextPair : nextSeries.getEvents() )
            {
                assertTrue( nextPair.getValue().equals( Pair.of( nextValue, nextValue ) ) );
                nextValue++;
            }
        }
    }

    @Test
    public void testTimeSeriesAreImmutable()
    {
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 3.0 ) ) );
        final SampleMetadata meta = SampleMetadata.of();

        PoolOfPairsBuilder<Double, Double> d = new PoolOfPairsBuilder<>();
        TimeSeries<Pair<Double, Double>> firstSeries = TimeSeries.of( firstBasisTime,
                                                                      first );
        PoolOfPairs<Double, Double> ts =
                d.addTimeSeries( firstSeries )
                 .setMetadata( meta )
                 .build();

        //Mutate 
        Iterator<TimeSeries<Pair<Double, Double>>> immutableSeries = ts.get().iterator();
        immutableSeries.next();

        assertThrows( UnsupportedOperationException.class, () -> immutableSeries.remove() );
    }

    @Test
    public void testToString()
    {
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        Instant basisTime = Instant.parse( FIRST_TIME );
        SampleMetadata meta = SampleMetadata.of();
        for ( int i = 0; i < 5; i++ )
        {
            String validTime = "1985-01-01T" + String.format( "%02d", i ) + ZERO_ZEE;

            first.add( Event.of( Instant.parse( validTime ),
                                 Pair.of( 1.0, 1.0 ) ) );
        }

        TimeSeries<Pair<Double, Double>> firstSeries =
                new TimeSeriesBuilder<Pair<Double, Double>>().addReferenceTime( basisTime, ReferenceTimeType.UNKNOWN )
                                                             .addEvents( first )
                                                             .setTimeScale( TimeScale.of() )
                                                             .build();


        b.addTimeSeries( firstSeries ).setMetadata( meta );

        String expectedOne = "UNKNOWN=1985-01-01T00:00:00Z";
        String expectedTwo = "(1985-01-01T00:00:00Z,(1.0,1.0))";
        String expectedThree = "(1985-01-01T01:00:00Z,(1.0,1.0))";
        String expectedFour = "(1985-01-01T02:00:00Z,(1.0,1.0))";
        String expectedFive = "(1985-01-01T03:00:00Z,(1.0,1.0))";
        String expectedSix = "(1985-01-01T04:00:00Z,(1.0,1.0))";
        String expectedSeven = "INSTANTANEOUS";

        String bToString = b.build()
                            .toString();
        assertTrue( ( bToString ).contains( expectedOne ) );
        assertTrue( ( bToString ).contains( expectedTwo) );
        assertTrue( ( bToString ).contains( expectedThree ) );
        assertTrue( ( bToString ).contains( expectedFour ) );
        assertTrue( ( bToString ).contains( expectedFive ) );
        assertTrue( ( bToString ).contains( expectedSix ) );
        assertTrue( ( bToString ).contains( expectedSeven ) );

        //Add another time-series
        Instant nextBasisTime = Instant.parse( FIFTH_TIME );
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        for ( int i = 0; i < 5; i++ )
        {
            String validTime = "1985-01-02T" + String.format( "%02d", i ) + ZERO_ZEE;

            second.add( Event.of( Instant.parse( validTime ),
                                  Pair.of( 1.0, 1.0 ) ) );
        }

        TimeSeries<Pair<Double, Double>> secondSeries =
                new TimeSeriesBuilder<Pair<Double, Double>>().addReferenceTime( nextBasisTime,
                                                                                ReferenceTimeType.UNKNOWN )
                                                             .addEvents( second )
                                                             .setTimeScale( TimeScale.of() )
                                                             .build();

        b.addTimeSeries( secondSeries );

        String expectedFirstOne = "UNKNOWN=1985-01-01T00:00:00Z";
        String expectedFirstTwo = "(1985-01-01T00:00:00Z,(1.0,1.0))";

        String expectedSecondOne = "UNKNOWN=1985-01-02T00:00:00Z";
        String expectedSecondTwo = "(1985-01-02T00:00:00Z,(1.0,1.0)";

        String bToStringAgain = b.build()
                                 .toString();
        assertTrue( bToStringAgain.contains( expectedFirstOne ) );
        assertTrue( bToStringAgain.contains( expectedFirstTwo ) );

        assertTrue( bToStringAgain.contains( expectedSecondOne ) );
        assertTrue( bToStringAgain.contains( expectedSecondTwo ) );
    }

    @Test
    public void testIterateIrregularTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> fourth = new TreeSet<>();

        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ), Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ), Pair.of( 3.0, 3.0 ) ) );
        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( Instant.parse( SIXTH_TIME ), Pair.of( 4.0, 4.0 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T04:00:00Z" ), Pair.of( 5.0, 5.0 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), Pair.of( 6.0, 6.0 ) ) );
        Instant thirdBasisTime = Instant.parse( SEVENTH_TIME );
        third.add( Event.of( Instant.parse( EIGHTH_TIME ), Pair.of( 7.0, 7.0 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T08:00:00Z" ), Pair.of( 8.0, 8.0 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T09:00:00Z" ), Pair.of( 9.0, 9.0 ) ) );
        Instant fourthBasisTime = Instant.parse( "1985-01-04T00:00:00Z" );
        fourth.add( Event.of( Instant.parse( "1985-01-04T02:00:00Z" ),
                              Pair.of( 10.0, 10.0 ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T04:00:00Z" ),
                              Pair.of( 11.0, 11.0 ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T06:00:00Z" ),
                              Pair.of( 12.0, 12.0 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        //Add the time-series, with only one for baseline
        PoolOfPairs<Double, Double> ts =
                b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                first ) )
                 .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                second ) )
                 .addTimeSeries( TimeSeries.of( thirdBasisTime,
                                                third ) )
                 .addTimeSeries( TimeSeries.of( fourthBasisTime,
                                                fourth ) )
                 .setMetadata( meta )
                 .build();

        //Iterate and test
        double[] expectedOrder = new double[] { 1, 7, 4, 10, 5, 11, 6, 12, 2, 8, 3, 9 };
        int nextIndex = 0;

        SortedSet<Duration> durations = new TreeSet<>();
        durations.add( Duration.ofHours( 1 ) );
        durations.add( Duration.ofHours( 2 ) );
        durations.add( Duration.ofHours( 4 ) );
        durations.add( Duration.ofHours( 6 ) );
        durations.add( Duration.ofHours( 8 ) );
        durations.add( Duration.ofHours( 9 ) );

        for ( Duration nextDuration : durations )
        {
            TimeWindow window = TimeWindow.of( nextDuration, nextDuration );

            for ( TimeSeries<Pair<Double, Double>> next : ts.get() )
            {
                TimeSeries<Pair<Double, Double>> filtered = TimeSeriesSlicer.filter( next, window );

                for ( Event<Pair<Double, Double>> nextPair : filtered.getEvents() )
                {
                    assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                                nextPair.getValue()
                                        .equals( Pair.of( expectedOrder[nextIndex],
                                                          expectedOrder[nextIndex] ) ) );
                    nextIndex++;
                }

            }
        }
    }

    @Test
    public void testIterateNonForecasts()
    {
        SortedSet<Event<Pair<Double, Double>>> data = new TreeSet<>();
        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        data.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        data.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 2.0 ) ) );
        data.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 3.0 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ), Pair.of( 4.0, 4.0 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ), Pair.of( 5.0, 5.0 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ), Pair.of( 6.0, 6.0 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T07:00:00Z" ), Pair.of( 7.0, 7.0 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ), Pair.of( 8.0, 8.0 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ), Pair.of( 9.0, 9.0 ) ) );

        SampleMetadata meta = SampleMetadata.of();

        //Add the time-series, with only one for baseline
        PoolOfPairs<Double, Double> ts =
                b.addTimeSeries( TimeSeries.of( data ) )
                 .setMetadata( meta )
                 .build();

        // Iterate by time
        double i = 1.0;
        for ( TimeSeries<Pair<Double, Double>> nextSeries : ts.get() )
        {
            for ( Event<Pair<Double, Double>> nextPair : nextSeries.getEvents() )
            {
                assertEquals( nextPair.getValue(), Pair.of( i, i ) );
                i++;
            }
        }

        assertEquals( 10, i, 0.0001 ); // All elements iterated

        // Iterate by basis time
        double j = 1.0;
        for ( TimeSeries<Pair<Double, Double>> tsn : ts.get() )
        {
            assertEquals( tsn.getEvents().first().getValue(), Pair.of( j, j ) );
            j++;
        }
        assertEquals( 2, j, 0.0001 ); // All elements iterated
    }

    @Test
    public void testEquals()
    {


    }

    @Test
    public void testHashcode()
    {
        // Equal objects have the same hashcode
        SortedSet<Event<Pair<Double, Double>>> data = new TreeSet<>();
        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        data.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        data.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 2.0 ) ) );
        data.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 3.0 ) ) );

        SampleMetadata meta = SampleMetadata.of();

        PoolOfPairs<Double, Double> one = b.addTimeSeries( TimeSeries.of( data ) )
                                           .setMetadata( meta )
                                           .build();


        assertEquals( one, one );
        assertEquals( one.hashCode(), one.hashCode() );

        // Consistent when invoked multiple times
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( one.hashCode(), one.hashCode() );
        }

    }


}

package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Tests the {@link TimeSeriesSlicer}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesSlicerTest
{

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";
    private static final String SECOND_TIME = "1985-01-01T01:00:00Z";
    private static final String THIRD_TIME = "1985-01-01T02:00:00Z";
    private static final String FOURTH_TIME = "1985-01-01T03:00:00Z";
    private static final String FIFTH_TIME = "1985-01-02T00:00:00Z";
    private static final String SIXTH_TIME = "1985-01-02T01:00:00Z";
    private static final String SEVENTH_TIME = "1985-01-02T02:00:00Z";
    private static final String EIGHTH_TIME = "1985-01-02T03:00:00Z";
    private static final String NINTH_TIME = "1985-01-03T00:00:00Z";
    private static final String TENTH_TIME = "1985-01-03T01:00:00Z";
    private static final String ELEVENTH_TIME = "1985-01-03T02:00:00Z";
    private static final String TWELFTH_TIME = "1985-01-03T03:00:00Z";
    private static final String THIRTEENTH_TIME = "2086-05-01T00:00:00Z";

    @Test
    public void testGetReferenceTimes()
    {
        //Build a time-series with two basis times
        SortedSet<Event<Pair<Double, Double>>> values = new TreeSet<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );

        List<TimeSeries<Pair<Double, Double>>> timeSeries = new ArrayList<>();

        timeSeries.add( TimeSeries.of( basisTime, values ) );

        Instant nextBasisTime = Instant.parse( SECOND_TIME );
        SortedSet<Event<Pair<Double, Double>>> otherValues = new TreeSet<>();
        otherValues.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );

        timeSeries.add( TimeSeries.of( nextBasisTime, otherValues ) );

        //Check dataset count
        SortedSet<Instant> times = TimeSeriesSlicer.getReferenceTimes( timeSeries, ReferenceTimeType.DEFAULT );
        assertEquals( 2, times.size() );

        //Check the basis times
        assertEquals( basisTime, times.first() );
        Iterator<Instant> it = times.iterator();
        it.next();
        assertTrue( it.next().equals( nextBasisTime ) );
    }

    @Test
    public void testGetDurations()
    {
        //Build a time-series with one basis time
        List<TimeSeries<Pair<Double, Double>>> timeSeries = new ArrayList<>();

        SortedSet<Event<Pair<Double, Double>>> values = new TreeSet<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        values.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 2.0 ) ) );
        values.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 3.0 ) ) );

        timeSeries.add( TimeSeries.of( basisTime, values ) );

        //Check dataset count
        SortedSet<Duration> actual = TimeSeriesSlicer.getDurations( timeSeries, ReferenceTimeType.DEFAULT );
        SortedSet<Duration> expected = new TreeSet<>();
        expected.add( Duration.ofHours( 1 ) );
        expected.add( Duration.ofHours( 2 ) );
        expected.add( Duration.ofHours( 3 ) );

        assertEquals( expected, actual );

    }

    @Test
    public void testFilterByReferenceTime()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 3.0 ) ) );
        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( Instant.parse( SIXTH_TIME ), Pair.of( 4.0, 4.0 ) ) );
        second.add( Event.of( Instant.parse( SEVENTH_TIME ), Pair.of( 5.0, 5.0 ) ) );
        second.add( Event.of( Instant.parse( EIGHTH_TIME ), Pair.of( 6.0, 6.0 ) ) );
        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( Instant.parse( TENTH_TIME ), Pair.of( 7.0, 7.0 ) ) );
        third.add( Event.of( Instant.parse( ELEVENTH_TIME ), Pair.of( 8.0, 8.0 ) ) );
        third.add( Event.of( Instant.parse( TWELFTH_TIME ), Pair.of( 9.0, 9.0 ) ) );

        //Add the time-series
        List<TimeSeries<Pair<Double, Double>>> ts = List.of( TimeSeries.of( firstBasisTime,
                                                                            first ),
                                                             TimeSeries.of( secondBasisTime,
                                                                            second ),
                                                             TimeSeries.of( thirdBasisTime,
                                                                            third ) );

        //Iterate and test
        List<TimeSeries<Pair<Double, Double>>> filtered =
                TimeSeriesSlicer.filterByReferenceTime( ts,
                                                        a -> a.equals( secondBasisTime ),
                                                        ReferenceTimeType.DEFAULT );

        SortedSet<Instant> referenceTimes = TimeSeriesSlicer.getReferenceTimes( filtered, ReferenceTimeType.DEFAULT );

        assertTrue( referenceTimes.size() == 1 );
        assertTrue( referenceTimes.first().equals( secondBasisTime ) );
        assertTrue( filtered.get( 0 )
                            .getEvents()
                            .first()
                            .getValue()
                            .equals( Pair.of( 4.0, 4.0 ) ) );

        //Check for empty output on none filter
        List<TimeSeries<Pair<Double, Double>>> pairs =
                TimeSeriesSlicer.filterByReferenceTime( ts,
                                                        a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ),
                                                        ReferenceTimeType.DEFAULT );

        SortedSet<Instant> sliced = TimeSeriesSlicer.getReferenceTimes( pairs, ReferenceTimeType.DEFAULT );

        assertTrue( sliced.isEmpty() );

        //Check exceptional cases
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filterByReferenceTime( (List<TimeSeries<Pair<Double, Double>>>) null,
                                                                    null,
                                                                    ReferenceTimeType.DEFAULT ) );
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filterByReferenceTime( ts, null, ReferenceTimeType.DEFAULT ) );
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filterByReferenceTime( ts,
                                                                    a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ),
                                                                    null ) );
    }

    @Test
    public void testGroupEventsByIntervalProducesThreeGroupsEachWithTwoEvents()
    {
        // Create the events
        SortedSet<Event<Double>> events = new TreeSet<>();

        Event<Double> one = Event.of( Instant.parse( "2079-12-03T00:00:01Z" ), 1.0 );
        Event<Double> two = Event.of( Instant.parse( "2079-12-03T02:00:00Z" ), 3.0 );
        Event<Double> three = Event.of( Instant.parse( "2079-12-03T04:00:00Z" ), 4.0 );
        Event<Double> four = Event.of( Instant.parse( "2079-12-03T05:00:00Z" ), 5.0 );
        Event<Double> five = Event.of( Instant.parse( "2079-12-03T19:00:01Z" ), 14.0 );
        Event<Double> six = Event.of( Instant.parse( "2079-12-03T21:00:00Z" ), 21.0 );

        events.add( one );
        events.add( two );
        events.add( three );
        events.add( four );
        events.add( five );
        events.add( six );

        // Create the period
        Duration period = Duration.ofHours( 3 );

        // Create the endsAt times
        Set<Instant> endsAt = new HashSet<>();
        Instant first = Instant.parse( "2079-12-03T03:00:00Z" );
        Instant second = Instant.parse( "2079-12-03T05:00:00Z" );
        Instant third = Instant.parse( "2079-12-03T21:00:00Z" );

        endsAt.add( first );
        endsAt.add( second );
        endsAt.add( third );

        Map<Instant, SortedSet<Event<Double>>> actual =
                TimeSeriesSlicer.groupEventsByInterval( events, endsAt, period );

        Map<Instant, SortedSet<Event<Double>>> expected = new HashMap<>();

        SortedSet<Event<Double>> groupOne = new TreeSet<>();
        groupOne.add( one );
        groupOne.add( two );
        expected.put( first, groupOne );

        SortedSet<Event<Double>> groupTwo = new TreeSet<>();
        groupTwo.add( three );
        groupTwo.add( four );
        expected.put( second, groupTwo );

        SortedSet<Event<Double>> groupThree = new TreeSet<>();
        groupThree.add( five );
        groupThree.add( six );
        expected.put( third, groupThree );

        assertEquals( expected, actual );

    }

    @Test
    public void testDecomposeWithoutLabelsProducesFourTraces()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = Instant.parse( THIRTEENTH_TIME );

        TimeSeries<Ensemble> ensemble =
                new TimeSeriesBuilder<Ensemble>()
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ),
                                                                      Ensemble.of( 1, 2, 3, 4 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ),
                                                                      Ensemble.of( 5, 6, 7, 8 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ),
                                                                      Ensemble.of( 9, 10, 11, 12 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ),
                                                                      Ensemble.of( 13, 14, 15, 16 ) ) )
                                                 .addReferenceTime( baseInstant,
                                                                    ReferenceTimeType.DEFAULT )
                                                 .build();

        List<TimeSeries<Double>> actual = TimeSeriesSlicer.decompose( ensemble );

        List<TimeSeries<Double>> expected = new ArrayList<>();

        TimeSeries<Double> one =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 1.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 5.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 9.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 13.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 4.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 8.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 12.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 16.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( four );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeWithLabelsProducesFourTraces()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = Instant.parse( THIRTEENTH_TIME );

        String[] labels = new String[] { "a", "b", "c", "d" };

        TimeSeries<Ensemble> ensemble =
                new TimeSeriesBuilder<Ensemble>()
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ),
                                                                      Ensemble.of( new double[] { 1, 2, 3, 4 },
                                                                                   labels ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ),
                                                                      Ensemble.of( new double[] { 5, 6, 7, 8 },
                                                                                   labels ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ),
                                                                      Ensemble.of( new double[] { 9, 10, 11, 12 },
                                                                                   labels ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ),
                                                                      Ensemble.of( new double[] { 13, 14, 15, 16 },
                                                                                   labels ) ) )
                                                 .addReferenceTime( baseInstant,
                                                                    ReferenceTimeType.DEFAULT )
                                                 .build();

        List<TimeSeries<Double>> actual = TimeSeriesSlicer.decompose( ensemble );

        List<TimeSeries<Double>> expected = new ArrayList<>();

        TimeSeries<Double> one =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 1.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 5.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 9.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 13.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 4.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 8.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 12.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 16.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.DEFAULT )
                                               .build();

        expected.add( four );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeAndThenComposeWithoutLabelsProducesTheSameSeries()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = Instant.parse( THIRTEENTH_TIME );

        TimeSeries<Ensemble> expected =
                new TimeSeriesBuilder<Ensemble>()
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ),
                                                                      Ensemble.of( 1, 2, 3, 4 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ),
                                                                      Ensemble.of( 5, 6, 7, 8 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ),
                                                                      Ensemble.of( 9, 10, 11, 12 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ),
                                                                      Ensemble.of( 13, 14, 15, 16 ) ) )
                                                 .addReferenceTime( baseInstant,
                                                                    ReferenceTimeType.DEFAULT )
                                                 .build();

        TimeSeries<Ensemble> actual =
                TimeSeriesSlicer.compose( TimeSeriesSlicer.decompose( expected ), new TreeSet<>() );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeAndThenComposeWithLabelsProducesTheSameSeries()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = Instant.parse( THIRTEENTH_TIME );

        String[] labels = new String[] { "a", "b", "c", "d" };

        TimeSeries<Ensemble> expected =
                new TimeSeriesBuilder<Ensemble>()
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ),
                                                                      Ensemble.of( new double[] { 1, 2, 3, 4 },
                                                                                   labels ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ),
                                                                      Ensemble.of( new double[] { 5, 6, 7, 8 },
                                                                                   labels ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ),
                                                                      Ensemble.of( new double[] { 9, 10, 11, 12 },
                                                                                   labels ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ),
                                                                      Ensemble.of( new double[] { 13, 14, 15, 16 },
                                                                                   labels ) ) )
                                                 .addReferenceTime( baseInstant,
                                                                    ReferenceTimeType.DEFAULT )
                                                 .build();

        SortedSet<String> labelSet = Arrays.stream( labels ).collect( Collectors.toCollection( TreeSet::new ) );

        TimeSeries<Ensemble> actual =
                TimeSeriesSlicer.compose( TimeSeriesSlicer.decompose( expected ), labelSet );

        assertEquals( expected, actual );
    }


}

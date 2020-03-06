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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Tests the {@link TimeSeriesSlicer}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesSlicerTest
{

    private static final Instant T2010_01_01T15_00_00Z = Instant.parse( "2010-01-01T15:00:00Z" );
    private static final Instant T2010_01_01T12_00_00Z = Instant.parse( "2010-01-01T12:00:00Z" );
    private static final Instant T1985_01_01T00_00_00Z = Instant.parse( "1985-01-01T00:00:00Z" );
    private static final Instant T1985_01_01T01_00_00Z = Instant.parse( "1985-01-01T01:00:00Z" );
    private static final Instant T1985_01_01T02_00_00Z = Instant.parse( "1985-01-01T02:00:00Z" );
    private static final Instant T1985_01_01T03_00_00Z = Instant.parse( "1985-01-01T03:00:00Z" );
    private static final Instant T1985_01_02T00_00_00Z = Instant.parse( "1985-01-02T00:00:00Z" );
    private static final Instant T1985_01_02T01_00_00Z = Instant.parse( "1985-01-02T01:00:00Z" );
    private static final Instant T1985_01_02T02_00_00Z = Instant.parse( "1985-01-02T02:00:00Z" );
    private static final Instant T1985_01_02T03_00_00Z = Instant.parse( "1985-01-02T03:00:00Z" );
    private static final Instant T1985_01_03T00_00_00Z = Instant.parse( "1985-01-03T00:00:00Z" );
    private static final Instant T1985_01_03T01_00_00Z = Instant.parse( "1985-01-03T01:00:00Z" );
    private static final Instant T1985_01_03T02_00_00Z = Instant.parse( "1985-01-03T02:00:00Z" );
    private static final Instant T1985_01_03T03_00_00Z = Instant.parse( "1985-01-03T03:00:00Z" );
    private static final Instant T2086_05_01T00_00_00Z = Instant.parse( "2086-05-01T00:00:00Z" );

    @Test
    public void testFilterByReferenceTime()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();

        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        Instant secondBasisTime = T1985_01_02T00_00_00Z;
        second.add( Event.of( T1985_01_02T01_00_00Z, Pair.of( 4.0, 4.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z, Pair.of( 5.0, 5.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z, Pair.of( 6.0, 6.0 ) ) );
        Instant thirdBasisTime = T1985_01_03T00_00_00Z;
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 7.0, 7.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 8.0, 8.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 9.0, 9.0 ) ) );

        //Add the time-series
        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( firstBasisTime, first );
        TimeSeries<Pair<Double, Double>> two = TimeSeries.of( secondBasisTime, second );
        TimeSeries<Pair<Double, Double>> three = TimeSeries.of( thirdBasisTime, third );

        //Iterate and test
        TimeSeries<Pair<Double, Double>> filteredOne =
                TimeSeriesSlicer.filter( one,
                                         TimeWindow.of( secondBasisTime,
                                                        secondBasisTime,
                                                        TimeWindow.DURATION_MIN,
                                                        TimeWindow.DURATION_MAX ) );

        assertEquals( TimeSeries.of(), filteredOne );

        TimeSeries<Pair<Double, Double>> filteredTwo =
                TimeSeriesSlicer.filter( two,
                                         TimeWindow.of( secondBasisTime,
                                                        secondBasisTime,
                                                        TimeWindow.DURATION_MIN,
                                                        TimeWindow.DURATION_MAX ) );

        assertEquals( two, filteredTwo );

        TimeSeries<Pair<Double, Double>> filteredThree =
                TimeSeriesSlicer.filter( three,
                                         TimeWindow.of( secondBasisTime,
                                                        secondBasisTime,
                                                        TimeWindow.DURATION_MIN,
                                                        TimeWindow.DURATION_MAX ) );

        assertEquals( TimeSeries.of(), filteredThree );

        //Check exceptional cases
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filter( (TimeSeries<Object>) null, (TimeWindow) null ) );
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filter( one, (TimeWindow) null ) );
    }

    @Test
    public void testFilterByValidTime()
    {

        // Create the series to filter
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        Instant firstBasisTime = T1985_01_01T00_00_00Z;

        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( firstBasisTime, first );

        // Filter the series
        TimeSeries<Pair<Double, Double>> actual =
                TimeSeriesSlicer.filter( one,
                                         TimeWindow.of( T1985_01_01T01_00_00Z,
                                                        T1985_01_01T02_00_00Z ) );

        // Create the expected series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );

        TimeSeries<Pair<Double, Double>> expected = TimeSeries.of( firstBasisTime, expectedEvents );

        assertEquals( expected, actual );
    }

    @Test
    public void testFilterByDuration()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();
        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        Instant secondBasisTime = T1985_01_02T00_00_00Z;
        second.add( Event.of( T1985_01_02T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        Instant thirdBasisTime = T1985_01_03T00_00_00Z;
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        //Add the time-series, with only one for baseline
        PoolOfPairs<Double, Double> ts = b.addTimeSeries( TimeSeries.of( firstBasisTime, first ) )
                                          .addTimeSeries( TimeSeries.of( secondBasisTime, second ) )
                                          .addTimeSeries( TimeSeries.of( thirdBasisTime, third ) )
                                          .addTimeSeriesForBaseline( TimeSeries.of( firstBasisTime, first ) )
                                          .setMetadata( meta )
                                          .setMetadataForBaseline( meta )
                                          .build();

        //Iterate and test
        double nextValue = 1.0;

        SortedSet<Duration> durations = new TreeSet<>();
        durations.add( Duration.ofHours( 1 ) );
        durations.add( Duration.ofHours( 2 ) );
        durations.add( Duration.ofHours( 3 ) );

        //Three time-series
        assertEquals( 3, ts.get().size() );

        for ( Duration duration : durations )
        {
            TimeWindow window = TimeWindow.of( duration, duration );
            TimeSeries<Pair<Double, Double>> events =
                    TimeSeriesSlicer.filter( ts.get().get( 0 ), window );
            for ( Event<Pair<Double, Double>> nextPair : events.getEvents() )
            {
                assertTrue( nextPair.getValue().equals( Pair.of( nextValue, nextValue ) ) );
            }

            nextValue++;
        }

        //Check the regular duration of a time-series with one duration
        SortedSet<Event<Pair<Double, Double>>> fourth = new TreeSet<>();
        fourth.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        PoolOfPairsBuilder<Double, Double> bu = new PoolOfPairsBuilder<>();

        PoolOfPairs<Double, Double> durationCheck =
                bu.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                 fourth ) )
                  .setMetadata( meta )
                  .build();

        TimeSeries<Pair<Double, Double>> next = durationCheck.get().get( 0 );
        next = TimeSeriesSlicer.filter( next, TimeWindow.of( Duration.ofHours( 51 ), Duration.ofHours( 51 ) ) );

        Duration actualDuration = Duration.between( next.getReferenceTimes().values().iterator().next(),
                                                    next.getEvents().first().getTime() );

        assertEquals( Duration.ofHours( 51 ), actualDuration );
    }

    @Test
    public void testFilterByEvent()
    {

        // Create the series to filter
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        Instant firstBasisTime = T1985_01_01T00_00_00Z;

        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( firstBasisTime, first );

        // Filter the series
        TimeSeries<Pair<Double, Double>> actual =
                TimeSeriesSlicer.filterByEvent( one, event -> !event.getTime().equals( T1985_01_01T02_00_00Z ) );

        // Create the expected series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        expectedEvents.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        
        TimeSeries<Pair<Double, Double>> expected = TimeSeries.of( firstBasisTime, expectedEvents );

        assertEquals( expected, actual );
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
    public void testGroupEventsByOverlappingIntervalProducesTwoGroupsEachWithFourEvents()
    {
        // Create the events
        SortedSet<Event<Double>> events = new TreeSet<>();

        Event<Double> one = Event.of( Instant.parse( "2079-12-03T00:00:01Z" ), 1.0 );
        Event<Double> two = Event.of( Instant.parse( "2079-12-03T06:00:00Z" ), 3.0 );
        Event<Double> three = Event.of( Instant.parse( "2079-12-03T12:00:01Z" ), 4.0 );
        Event<Double> four = Event.of( Instant.parse( "2079-12-03T18:00:00Z" ), 5.0 );
        Event<Double> five = Event.of( Instant.parse( "2079-12-04T00:00:00Z" ), 14.0 );
        Event<Double> six = Event.of( Instant.parse( "2079-12-04T06:00:00Z" ), 21.0 );

        events.add( one );
        events.add( two );
        events.add( three );
        events.add( four );
        events.add( five );
        events.add( six );

        // Create the period
        Duration period = Duration.ofHours( 18 );

        // Create the endsAt times
        Set<Instant> endsAt = new HashSet<>();
        Instant first = Instant.parse( "2079-12-03T18:00:00Z" );
        Instant second = Instant.parse( "2079-12-04T06:00:00Z" );

        endsAt.add( first );
        endsAt.add( second );

        Map<Instant, SortedSet<Event<Double>>> actual =
                TimeSeriesSlicer.groupEventsByInterval( events, endsAt, period );

        Map<Instant, SortedSet<Event<Double>>> expected = new HashMap<>();

        SortedSet<Event<Double>> groupOne = new TreeSet<>();
        groupOne.add( one );
        groupOne.add( two );
        groupOne.add( three );
        groupOne.add( four );
        expected.put( first, groupOne );

        SortedSet<Event<Double>> groupTwo = new TreeSet<>();
        groupTwo.add( three );
        groupTwo.add( four );
        groupTwo.add( five );
        groupTwo.add( six );
        expected.put( second, groupTwo );

        assertEquals( expected, actual );

    }

    @Test
    public void testDecomposeWithoutLabelsProducesFourTraces()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;

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
                                                                    ReferenceTimeType.UNKNOWN )
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
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 4.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 8.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 12.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 16.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( four );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeWithLabelsProducesFourTraces()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;

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
                                                                    ReferenceTimeType.UNKNOWN )
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
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 4.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 8.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 12.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 16.0 ) )
                                               .addReferenceTime( baseInstant,
                                                                  ReferenceTimeType.UNKNOWN )
                                               .build();

        expected.add( four );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeAndThenComposeWithoutLabelsProducesTheSameSeries()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;

        TimeSeries<Ensemble> expected =
                new TimeSeriesBuilder<Ensemble>()
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ),
                                                                      Ensemble.of( 4, 3, 2, 1 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ),
                                                                      Ensemble.of( 5, 6, 7, 8 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ),
                                                                      Ensemble.of( 12, 11, 10, 9 ) ) )
                                                 .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ),
                                                                      Ensemble.of( 13, 15, 14, 16 ) ) )
                                                 .addReferenceTime( baseInstant,
                                                                    ReferenceTimeType.UNKNOWN )
                                                 .build();

        TimeSeries<Ensemble> actual =
                TimeSeriesSlicer.compose( TimeSeriesSlicer.decompose( expected ), new TreeSet<>() );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeAndThenComposeWithLabelsProducesTheSameSeries()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;

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
                                                                    ReferenceTimeType.UNKNOWN )
                                                 .build();

        SortedSet<String> labelSet = Arrays.stream( labels ).collect( Collectors.toCollection( TreeSet::new ) );

        TimeSeries<Ensemble> actual =
                TimeSeriesSlicer.compose( TimeSeriesSlicer.decompose( expected ), labelSet );

        assertEquals( expected, actual );
    }

    @Test
    public void testFilterTimeSeriesOfSingleValuedPairs()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();
        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 10.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 11.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 12.0 ) ) );

        Instant secondBasisTime = T1985_01_02T00_00_00Z;
        second.add( Event.of( T1985_01_02T01_00_00Z,
                              Pair.of( 4.0, 13.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z,
                              Pair.of( 5.0, 14.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z,
                              Pair.of( 6.0, 15.0 ) ) );

        Instant thirdBasisTime = T1985_01_03T00_00_00Z;
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 7.0, 16.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 8.0, 17.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 9.0, 18.0 ) ) );
        SampleMetadata meta = SampleMetadata.of();

        //Add the time-series
        PoolOfPairs<Double, Double> firstSeries = b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                  first ) )
                                                   .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                                                  second ) )
                                                   .addTimeSeries( TimeSeries.of( thirdBasisTime,
                                                                                  third ) )
                                                   .setMetadata( meta )
                                                   .build();

        // Filter all values where the left side is greater than 0
        PoolOfPairs<Double, Double> firstResult =
                TimeSeriesSlicer.filter( firstSeries,
                                         Slicer.left( value -> value > 0 ),
                                         null );

        assertTrue( firstResult.getRawData().equals( firstSeries.getRawData() ) );

        // Filter all values where the left side is greater than 3
        PoolOfPairs<Double, Double> secondResult =
                TimeSeriesSlicer.filter( firstSeries,
                                         Slicer.left( value -> value > 3 ),
                                         clim -> clim > 0 );

        List<Event<Pair<Double, Double>>> secondData = new ArrayList<>();
        secondResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( secondData::add ) );
        List<Event<Pair<Double, Double>>> secondBenchmark = new ArrayList<>();
        secondBenchmark.addAll( second );
        secondBenchmark.addAll( third );

        assertTrue( secondData.equals( secondBenchmark ) );

        // Add climatology for later
        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3, 4, 5, Double.NaN );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( 1, 2, 3, 4, 5 );

        b.setClimatology( climatology );

        // Filter all values where the left and right sides are both greater than or equal to 7
        PoolOfPairs<Double, Double> thirdResult =
                TimeSeriesSlicer.filter( firstSeries,
                                         Slicer.leftAndRight( value -> value >= 7 ),
                                         null );

        List<Event<Pair<Double, Double>>> thirdData = new ArrayList<>();
        thirdResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( thirdData::add ) );
        List<Event<Pair<Double, Double>>> thirdBenchmark = new ArrayList<>();
        thirdBenchmark.addAll( third );

        assertTrue( thirdData.equals( thirdBenchmark ) );

        // Filter on climatology simultaneously
        PoolOfPairs<Double, Double> fourthResult =
                TimeSeriesSlicer.filter( b.build(),
                                         Slicer.leftAndRight( value -> value > 7 ),
                                         Double::isFinite );

        assertTrue( fourthResult.getClimatology().equals( climatologyExpected ) );

        // Also filter baseline data
        b.addTimeSeriesForBaseline( TimeSeries.of( firstBasisTime, first ) )
         .addTimeSeriesForBaseline( TimeSeries.of( secondBasisTime, second ) )
         .setMetadataForBaseline( meta );

        // Filter all values where both sides are greater than or equal to 4
        PoolOfPairs<Double, Double> fifthResult =
                TimeSeriesSlicer.filter( b.build(),
                                         Slicer.left( value -> value >= 4 ),
                                         clim -> clim > 0 );

        List<Event<Pair<Double, Double>>> fifthData = new ArrayList<>();
        fifthResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( fifthData::add ) );

        // Same as second benchmark for main data
        assertTrue( fifthData.equals( secondBenchmark ) );

        // Baseline data
        List<Event<Pair<Double, Double>>> fifthDataBase = new ArrayList<>();
        fifthResult.getBaselineData()
                   .get()
                   .forEach( nextSeries -> nextSeries.getEvents().forEach( fifthDataBase::add ) );
        List<Event<Pair<Double, Double>>> fifthBenchmarkBase = new ArrayList<>();
        fifthBenchmarkBase.addAll( second );

        assertTrue( fifthDataBase.equals( fifthBenchmarkBase ) );

    }

    @Test
    public void testSnipWithBufferOnLowerBoundAndUpperBound()
    {
        // Build a time-series to snip
        SortedSet<Event<Double>> first = new TreeSet<>();

        first.add( Event.of( Instant.parse( "2010-01-01T13:00:00Z" ), 1.0 ) );
        first.add( Event.of( Instant.parse( "2010-01-01T14:00:00Z" ), 2.0 ) );
        first.add( Event.of( T2010_01_01T15_00_00Z, 3.0 ) );
        first.add( Event.of( Instant.parse( "2010-01-01T16:00:00Z" ), 4.0 ) );
        first.add( Event.of( Instant.parse( "2010-01-01T17:00:00Z" ), 5.0 ) );

        TimeSeries<Double> toSnip = TimeSeries.of( T2010_01_01T12_00_00Z, first );

        // Build a time-series to snip against
        TimeSeries<Double> snipTo =
                TimeSeries.of( T2010_01_01T12_00_00Z,
                               new TreeSet<>( Set.of( Event.of( T2010_01_01T15_00_00Z,
                                                                6.0 ) ) ) );

        // Add a buffer of one time-step on the lower and upper boundaries
        Duration buffer = Duration.ofHours( 1 );

        // Snip
        TimeSeries<Double> actual = TimeSeriesSlicer.snip( toSnip, snipTo, buffer, buffer );

        // Create the expected series
        SortedSet<Event<Double>> expectedEvents = new TreeSet<>();

        expectedEvents.add( Event.of( Instant.parse( "2010-01-01T14:00:00Z" ), 2.0 ) );
        expectedEvents.add( Event.of( T2010_01_01T15_00_00Z, 3.0 ) );
        expectedEvents.add( Event.of( Instant.parse( "2010-01-01T16:00:00Z" ), 4.0 ) );

        TimeSeries<Double> expected = TimeSeries.of( T2010_01_01T12_00_00Z, expectedEvents );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeOffsetToValidTimes()
    {
        // Build a time-series to adjust
        SortedSet<Event<Double>> first = new TreeSet<>();
        first.add( Event.of( T2010_01_01T15_00_00Z, 3.0 ) );

        TimeSeries<Double> toAdjust = TimeSeries.of( T2010_01_01T12_00_00Z, first );

        // Add an offset of one hour
        Duration offset = Duration.ofHours( 1 );

        // Adjust
        TimeSeries<Double> actual = TimeSeriesSlicer.applyOffsetToValidTimes( toAdjust, offset );

        // Create the expected series
        SortedSet<Event<Double>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( Instant.parse( "2010-01-01T16:00:00Z" ), 3.0 ) );
        TimeSeries<Double> expected = TimeSeries.of( T2010_01_01T12_00_00Z, expectedEvents );

        assertEquals( expected, actual );
    }

    @Test
    public void testMapEventsByDuration()
    {
        // Build a time-series to adjust
        SortedSet<Event<Double>> first = new TreeSet<>();
        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        first.add( Event.of( T1985_01_01T01_00_00Z, 1.0 ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, 2.0 ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, 3.0 ) );

        //Add the time-series
        TimeSeries<Double> one = TimeSeries.of( firstBasisTime, ReferenceTimeType.T0, first );

        // Create the expected mapping
        Map<Duration, Event<Double>> expectedMapping = new HashMap<>();
        expectedMapping.put( Duration.ofHours( 1 ), Event.of( T1985_01_01T01_00_00Z, 1.0 ) );
        expectedMapping.put( Duration.ofHours( 2 ), Event.of( T1985_01_01T02_00_00Z, 2.0 ) );
        expectedMapping.put( Duration.ofHours( 3 ), Event.of( T1985_01_01T03_00_00Z, 3.0 ) );

        Map<Duration, Event<Double>> actualMapping = TimeSeriesSlicer.mapEventsByDuration( one, ReferenceTimeType.T0 );

        assertEquals( expectedMapping, actualMapping );

        // Check that a missing reference time produces an empty mapping
        Map<Duration, Event<Double>> actualEmptyMapping =
                TimeSeriesSlicer.mapEventsByDuration( one, ReferenceTimeType.ANALYSIS_START_TIME );

        assertEquals( Map.of(), actualEmptyMapping );

    }

}

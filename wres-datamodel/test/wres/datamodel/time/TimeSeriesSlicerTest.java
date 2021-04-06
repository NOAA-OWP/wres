package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import wres.datamodel.FeatureKey;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.pairs.PoolOfPairs;
import wres.datamodel.pools.pairs.PoolOfPairs.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Tests the {@link TimeSeriesSlicer}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesSlicerTest
{

    private static final String CFS = "CFS";
    private static final String STREAMFLOW = "STREAMFLOW";
    private static final FeatureKey DRRC2 = FeatureKey.of( "DRRC2" );
    private static final String T2010_01_01T16_00_00Z = "2010-01-01T16:00:00Z";
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

    private static final String VARIABLE_NAME = "Fruit";
    private static final FeatureKey FEATURE_NAME = FeatureKey.of( "Tropics" );
    private static final String UNIT = "kg/h";

    @Test
    public void testFilterByReferenceTime()
    {
        //Build three time-series each with its own basis time
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

        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                        firstBasisTime ),
                                                                TimeScaleOuter
                                                                              .of( Duration.ofHours( 1 ) ),
                                                                STREAMFLOW,
                                                                DRRC2,
                                                                CFS );

        TimeSeriesMetadata metadataOneNoRefTimes = TimeSeriesMetadata.of( Map.of(),
                                                                          TimeScaleOuter
                                                                                        .of( Duration.ofHours( 1 ) ),
                                                                          STREAMFLOW,
                                                                          DRRC2,
                                                                          CFS );

        TimeSeriesMetadata metadataTwo = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                        secondBasisTime ),
                                                                TimeScaleOuter
                                                                              .of( Duration.ofHours( 1 ) ),
                                                                STREAMFLOW,
                                                                DRRC2,
                                                                CFS );

        TimeSeriesMetadata metadataThree = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                          thirdBasisTime ),
                                                                  TimeScaleOuter
                                                                                .of( Duration.ofHours( 1 ) ),
                                                                  STREAMFLOW,
                                                                  DRRC2,
                                                                  CFS );

        TimeSeriesMetadata metadataThreeNoRefTimes = TimeSeriesMetadata.of( Map.of(),
                                                                            TimeScaleOuter
                                                                                          .of( Duration.ofHours( 1 ) ),
                                                                            STREAMFLOW,
                                                                            DRRC2,
                                                                            CFS );
        //Add the time-series
        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( metadataOne, first );
        TimeSeries<Pair<Double, Double>> two = TimeSeries.of( metadataTwo, second );
        TimeSeries<Pair<Double, Double>> three = TimeSeries.of( metadataThree, third );

        //Iterate and test
        TimeSeries<Pair<Double, Double>> filteredOne =
                TimeSeriesSlicer.filter( one,
                                         TimeWindowOuter.of( secondBasisTime,
                                                             secondBasisTime,
                                                             TimeWindowOuter.DURATION_MIN,
                                                             TimeWindowOuter.DURATION_MAX ) );

        assertEquals( TimeSeries.of( metadataOneNoRefTimes ), filteredOne );

        TimeSeries<Pair<Double, Double>> filteredTwo =
                TimeSeriesSlicer.filter( two,
                                         TimeWindowOuter.of( secondBasisTime,
                                                             secondBasisTime,
                                                             TimeWindowOuter.DURATION_MIN,
                                                             TimeWindowOuter.DURATION_MAX ) );

        assertEquals( two, filteredTwo );

        TimeSeries<Pair<Double, Double>> filteredThree =
                TimeSeriesSlicer.filter( three,
                                         TimeWindowOuter.of( secondBasisTime,
                                                             secondBasisTime,
                                                             TimeWindowOuter.DURATION_MIN,
                                                             TimeWindowOuter.DURATION_MAX ) );

        assertEquals( TimeSeries.of( metadataThreeNoRefTimes ), filteredThree );

        //Check exceptional cases
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filter( (TimeSeries<Object>) null, (TimeWindowOuter) null ) );
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filter( one, (TimeWindowOuter) null ) );
    }

    @Test
    public void testFilterByValidTime()
    {

        // Create the series to filter
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( firstBasisTime );
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( firstMetadata, first );

        // Filter the series
        TimeSeries<Pair<Double, Double>> actual =
                TimeSeriesSlicer.filter( one,
                                         TimeWindowOuter.of( T1985_01_01T01_00_00Z,
                                                             T1985_01_01T02_00_00Z ) );

        // Create the expected series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );

        TimeSeries<Pair<Double, Double>> expected = TimeSeries.of( firstMetadata, expectedEvents );

        assertEquals( expected, actual );
    }

    @Test
    public void testFilterByDuration()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();
        Builder<Double, Double> b = new Builder<>();

        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( firstBasisTime );
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        Instant secondBasisTime = T1985_01_02T00_00_00Z;
        TimeSeriesMetadata secondMetadata = getBoilerplateMetadataWithT0( secondBasisTime );
        second.add( Event.of( T1985_01_02T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        Instant thirdBasisTime = T1985_01_03T00_00_00Z;
        TimeSeriesMetadata thirdMetadata = getBoilerplateMetadataWithT0( thirdBasisTime );
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        PoolMetadata meta = PoolMetadata.of();
        PoolMetadata baseMeta = PoolMetadata.of( true );
        //Add the time-series, with only one for baseline
        PoolOfPairs<Double, Double> ts = b.addTimeSeries( TimeSeries.of( firstMetadata, first ) )
                                          .addTimeSeries( TimeSeries.of( secondMetadata, second ) )
                                          .addTimeSeries( TimeSeries.of( thirdMetadata, third ) )
                                          .addTimeSeriesForBaseline( TimeSeries.of( firstMetadata, first ) )
                                          .setMetadata( meta )
                                          .setMetadataForBaseline( baseMeta )
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
            TimeWindowOuter window = TimeWindowOuter.of( duration, duration );
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

        Builder<Double, Double> bu = new Builder<>();

        PoolOfPairs<Double, Double> durationCheck =
                bu.addTimeSeries( TimeSeries.of( firstMetadata,
                                                 fourth ) )
                  .setMetadata( meta )
                  .build();

        TimeSeries<Pair<Double, Double>> next = durationCheck.get().get( 0 );
        next = TimeSeriesSlicer.filter( next, TimeWindowOuter.of( Duration.ofHours( 51 ), Duration.ofHours( 51 ) ) );

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
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( firstBasisTime );
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( metadata, first );

        // Filter the series
        TimeSeries<Pair<Double, Double>> actual =
                TimeSeriesSlicer.filterByEvent( one, event -> !event.getTime().equals( T1985_01_01T02_00_00Z ) );

        // Create the expected series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        expectedEvents.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        TimeSeries<Pair<Double, Double>> expected = TimeSeries.of( metadata, expectedEvents );

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
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( baseInstant );
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
                                                 .setMetadata( metadata )
                                                 .build();

        List<TimeSeries<Double>> actual = TimeSeriesSlicer.decompose( ensemble );

        List<TimeSeries<Double>> expected = new ArrayList<>();

        TimeSeries<Double> one =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 1.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 5.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 9.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 13.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 4.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 8.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 12.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 16.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( four );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeWithLabelsProducesFourTraces()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;

        Labels labels = Labels.of( new String[] { "a", "b", "c", "d" } );
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( baseInstant );
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
                                                 .setMetadata( metadata )
                                                 .build();

        List<TimeSeries<Double>> actual = TimeSeriesSlicer.decompose( ensemble );

        List<TimeSeries<Double>> expected = new ArrayList<>();

        TimeSeries<Double> one =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 1.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 5.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 9.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 13.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeriesBuilder<Double>()
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 4.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 8.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 12.0 ) )
                                               .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 16.0 ) )
                                               .setMetadata( metadata )
                                               .build();

        expected.add( four );

        assertEquals( expected, actual );
    }

    @Test
    public void testDecomposeAndThenComposeWithoutLabelsProducesTheSameSeries()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;
        TimeSeriesMetadata expectedMetadata =
                getBoilerplateMetadataWithT0( baseInstant );
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
                                                 .setMetadata( expectedMetadata )
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

        Labels labels = Labels.of( new String[] { "a", "b", "c", "d" } );
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( baseInstant );
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
                                                 .setMetadata( metadata )
                                                 .build();

        SortedSet<String> labelSet = Arrays.stream( labels.getLabels() )
                                           .collect( Collectors.toCollection( TreeSet::new ) );

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
        Builder<Double, Double> b = new Builder<>();

        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( firstBasisTime );
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 10.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 11.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 12.0 ) ) );

        Instant secondBasisTime = T1985_01_02T00_00_00Z;
        TimeSeriesMetadata secondMetadata = getBoilerplateMetadataWithT0( secondBasisTime );
        second.add( Event.of( T1985_01_02T01_00_00Z,
                              Pair.of( 4.0, 13.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z,
                              Pair.of( 5.0, 14.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z,
                              Pair.of( 6.0, 15.0 ) ) );

        Instant thirdBasisTime = T1985_01_03T00_00_00Z;
        TimeSeriesMetadata thirdMetadata = getBoilerplateMetadataWithT0( thirdBasisTime );
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 7.0, 16.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 8.0, 17.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 9.0, 18.0 ) ) );
        PoolMetadata meta = PoolMetadata.of();

        //Add the time-series
        PoolOfPairs<Double, Double> firstSeries = b.addTimeSeries( TimeSeries.of( firstMetadata,
                                                                                  first ) )
                                                   .addTimeSeries( TimeSeries.of( secondMetadata,
                                                                                  second ) )
                                                   .addTimeSeries( TimeSeries.of( thirdMetadata,
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
        PoolMetadata baseMeta = PoolMetadata.of( true );
        b.addTimeSeriesForBaseline( TimeSeries.of( firstMetadata, first ) )
         .addTimeSeriesForBaseline( TimeSeries.of( secondMetadata, second ) )
         .setMetadataForBaseline( baseMeta );

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
    public void testFilterTimeSeriesByEvent()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Double>> first = new TreeSet<>();

        Instant firstBasisTime = T1985_01_01T00_00_00Z;
        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( firstBasisTime );
        first.add( Event.of( T1985_01_01T01_00_00Z, 1.0 ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, 2.0 ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, 3.0 ) );

        //Add the time-series
        TimeSeries<Double> series = TimeSeries.of( firstMetadata, first );

        // Filter all values where the valid time is not 1985-01-01T02:00:00Z
        TimeSeries<Double> actual = TimeSeriesSlicer.filterByEvent( series,
                                                                    next -> next.getTime()
                                                                                .equals( T1985_01_01T02_00_00Z ) );

        TimeSeries<Double> expected = new TimeSeriesBuilder<Double>().addEvent( Event.of( T1985_01_01T02_00_00Z, 2.0 ) )
                                                                     .setMetadata( firstMetadata )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testSnipWithBufferOnLowerBoundAndUpperBound()
    {
        // Build a time-series to snip
        SortedSet<Event<Double>> first = new TreeSet<>();

        first.add( Event.of( Instant.parse( "2010-01-01T13:00:00Z" ), 1.0 ) );
        first.add( Event.of( Instant.parse( "2010-01-01T14:00:00Z" ), 2.0 ) );
        first.add( Event.of( T2010_01_01T15_00_00Z, 3.0 ) );
        first.add( Event.of( Instant.parse( T2010_01_01T16_00_00Z ), 4.0 ) );
        first.add( Event.of( Instant.parse( "2010-01-01T17:00:00Z" ), 5.0 ) );
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( T2010_01_01T12_00_00Z );
        TimeSeries<Double> toSnip = TimeSeries.of( metadata, first );

        // Build a time-series to snip against
        TimeSeries<Double> snipTo =
                TimeSeries.of( metadata,
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
        expectedEvents.add( Event.of( Instant.parse( T2010_01_01T16_00_00Z ), 4.0 ) );

        TimeSeries<Double> expected = TimeSeries.of( metadata, expectedEvents );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeOffsetToValidTimes()
    {
        // Build a time-series to adjust
        SortedSet<Event<Double>> first = new TreeSet<>();
        first.add( Event.of( T2010_01_01T15_00_00Z, 3.0 ) );
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( T2010_01_01T12_00_00Z );
        TimeSeries<Double> toAdjust = TimeSeries.of( metadata, first );

        // Add an offset of one hour
        Duration offset = Duration.ofHours( 1 );

        // Adjust
        TimeSeries<Double> actual = TimeSeriesSlicer.applyOffsetToValidTimes( toAdjust, offset );

        // Create the expected series
        SortedSet<Event<Double>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( Instant.parse( T2010_01_01T16_00_00Z ), 3.0 ) );
        TimeSeries<Double> expected = TimeSeries.of( metadata, expectedEvents );

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
        TimeSeriesMetadata metadata =
                getBoilerplateMetadataWithT0( firstBasisTime );
        TimeSeries<Double> one = TimeSeries.of( metadata, first );

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

    @Test
    public void testConsolidateTimeSeriesWithZeroReferenceTimes()
    {
        TimeSeries<Double> one =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ),
                                                                    5.812511 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ),
                                                                    6.759735 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T01:00:00Z" ),
                                                                    7.3409863 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T02:00:00Z" ),
                                                                    7.9222374 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T03:00:00Z" ),
                                                                    8.503489 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T04:00:00Z" ),
                                                                    9.08474 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T05:00:00Z" ),
                                                                    9.665991 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ),
                                                                    10.247243 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T07:00:00Z" ),
                                                                    9.52606 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T08:00:00Z" ),
                                                                    8.804878 ) )
                                               .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                               .build();

        TimeSeries<Double> two =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ),
                                                                    10.247243 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T07:00:00Z" ),
                                                                    9.52606 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T08:00:00Z" ),
                                                                    8.804878 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T09:00:00Z" ),
                                                                    8.083696 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T10:00:00Z" ),
                                                                    7.3517504 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T11:00:00Z" ),
                                                                    6.6305685 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T12:00:00Z" ),
                                                                    5.9093866 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T13:00:00Z" ),
                                                                    5.489594 ) )
                                               .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                               .build();

        Collection<TimeSeries<Double>> actual =
                TimeSeriesSlicer.consolidateTimeSeriesWithZeroReferenceTimes( List.of( one, two ) );

        assertEquals( 2, actual.size() );

        TimeSeries<Double> expectedOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "1988-10-04T23:00:00Z" ),
                                                                    5.812511 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ),
                                                                    6.759735 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T01:00:00Z" ),
                                                                    7.3409863 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T02:00:00Z" ),
                                                                    7.9222374 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T03:00:00Z" ),
                                                                    8.503489 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T04:00:00Z" ),
                                                                    9.08474 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T05:00:00Z" ),
                                                                    9.665991 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ),
                                                                    10.247243 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T07:00:00Z" ),
                                                                    9.52606 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T08:00:00Z" ),
                                                                    8.804878 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T09:00:00Z" ),
                                                                    8.083696 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T10:00:00Z" ),
                                                                    7.3517504 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T11:00:00Z" ),
                                                                    6.6305685 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T12:00:00Z" ),
                                                                    5.9093866 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T13:00:00Z" ),
                                                                    5.489594 ) )
                                               .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                               .build();

        TimeSeries<Double> expectedTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ),
                                                                    10.247243 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T07:00:00Z" ),
                                                                    9.52606 ) )
                                               .addEvent( Event.of( Instant.parse( "1988-10-05T08:00:00Z" ),
                                                                    8.804878 ) )
                                               .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                               .build();

        assertEquals( List.of( expectedOne, expectedTwo ), actual );
    }

    @Test
    public void testFilterEnsembleForYearThatBeginsOnFirstOctober()
    {
        Instant validTime = Instant.parse( "1984-10-02T00:00:00Z" );
        Ensemble ensemble = Ensemble.of( new double[] { 1, 2, 3 }, Labels.of( "1984", "1985", "1986" ) );
        Event<Ensemble> toFilter = Event.of( validTime, ensemble );

        // New year starts on 1 October
        MonthDay startOfYear = MonthDay.of( 10, 1 );

        Event<Ensemble> actual = TimeSeriesSlicer.filter( toFilter, startOfYear );

        Ensemble expectedEnsemble = Ensemble.of( new double[] { 1, 3 }, Labels.of( "1984", "1986" ) );
        Event<Ensemble> expected = Event.of( validTime, expectedEnsemble );

        assertEquals( expected, actual );

        // Test for event before 1 October
        Instant validTimeTwo = Instant.parse( "1984-09-30T23:59:59Z" );
        Event<Ensemble> toFilterTwo = Event.of( validTimeTwo, ensemble );
        Event<Ensemble> actualTwo = TimeSeriesSlicer.filter( toFilterTwo, startOfYear );

        Ensemble expectedEnsembleTwo = Ensemble.of( new double[] { 2, 3 }, Labels.of( "1985", "1986" ) );
        Event<Ensemble> expectedTwo = Event.of( validTimeTwo, expectedEnsembleTwo );

        assertEquals( expectedTwo, actualTwo );
    }

    @Test
    public void testFilterEnsembleForYearThatBeginsOnFirstJanuary()
    {
        Instant validTime = Instant.parse( "1984-01-01T00:00:00Z" );
        Ensemble ensemble = Ensemble.of( new double[] { 1, 2, 3 }, Labels.of( "1984", "1985", "1986" ) );
        Event<Ensemble> toFilter = Event.of( validTime, ensemble );
        // New year starts on 1 January
        MonthDay startOfYear = MonthDay.of( 1, 1 );

        Event<Ensemble> actual = TimeSeriesSlicer.filter( toFilter, startOfYear );

        Ensemble expectedEnsemble = Ensemble.of( new double[] { 2, 3 }, Labels.of( "1985", "1986" ) );
        Event<Ensemble> expected = Event.of( validTime, expectedEnsemble );

        assertEquals( expected, actual );
    }

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0 )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

    private static TimeSeriesMetadata getBoilerplateMetadata()
    {
        return TimeSeriesMetadata.of( Map.of(),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

}

package wres.datamodel.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import wres.config.yaml.components.TimeInterval;
import wres.datamodel.types.Climatology;
import wres.datamodel.types.Ensemble;
import wres.datamodel.Slicer;
import wres.datamodel.types.Ensemble.Labels;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link TimeSeriesSlicer}.
 *
 * @author James Brown
 */
final class TimeSeriesSlicerTest
{
    private static final String CFS = "CFS";
    private static final String STREAMFLOW = "STREAMFLOW";
    private static final Feature DRRC2 = Feature.of(
            MessageUtilities.getGeometry( "DRRC2" ) );
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
    private static final Feature FEATURE_NAME = Feature.of( MessageUtilities.getGeometry( "Tropics" ) );
    private static final String UNIT = "kg/h";

    @Test
    void testFilterByReferenceTime()
    {
        //Build three time-series each with its own basis time
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();

        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        Instant secondBasisTime = T1985_01_02T00_00_00Z;
        second.add( Event.of( T1985_01_02T01_00_00Z, Pair.of( 4.0, 4.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z, Pair.of( 5.0, 5.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z, Pair.of( 6.0, 6.0 ) ) );
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 7.0, 7.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 8.0, 8.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 9.0, 9.0 ) ) );

        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                        T1985_01_01T00_00_00Z ),
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
                                                                          T1985_01_03T00_00_00Z ),
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
                                         TimeWindowOuter.of( MessageUtilities.getTimeWindow(
                                                 secondBasisTime,
                                                 secondBasisTime,
                                                 TimeWindowOuter.DURATION_MIN,
                                                 TimeWindowOuter.DURATION_MAX ) ) );

        assertEquals( TimeSeries.of( metadataOneNoRefTimes ), filteredOne );

        TimeSeries<Pair<Double, Double>> filteredTwo =
                TimeSeriesSlicer.filter( two,
                                         TimeWindowOuter.of( MessageUtilities.getTimeWindow(
                                                 secondBasisTime,
                                                 secondBasisTime,
                                                 TimeWindowOuter.DURATION_MIN,
                                                 TimeWindowOuter.DURATION_MAX ) ) );

        assertEquals( two, filteredTwo );

        TimeSeries<Pair<Double, Double>> filteredThree =
                TimeSeriesSlicer.filter( three,
                                         TimeWindowOuter.of( MessageUtilities.getTimeWindow(
                                                 secondBasisTime,
                                                 secondBasisTime,
                                                 TimeWindowOuter.DURATION_MIN,
                                                 TimeWindowOuter.DURATION_MAX ) ) );

        assertEquals( TimeSeries.of( metadataThreeNoRefTimes ), filteredThree );

        //Check exceptional cases
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filter( null, ( TimeWindowOuter ) null ) );
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filter( one, ( TimeWindowOuter ) null ) );
    }

    @Test
    void testFilterByValidTime()
    {

        // Create the series to filter
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( T1985_01_01T00_00_00Z );
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( firstMetadata, first );

        // Filter the series
        TimeWindow inner = MessageUtilities.getTimeWindow( T1985_01_01T01_00_00Z, T1985_01_01T02_00_00Z );
        TimeWindowOuter outer = TimeWindowOuter.of( inner );
        TimeSeries<Pair<Double, Double>> actual = TimeSeriesSlicer.filter( one, outer );

        // Create the expected series
        SortedSet<Event<Pair<Double, Double>>> expectedEvents = new TreeSet<>();
        expectedEvents.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );

        TimeSeries<Pair<Double, Double>> expected = TimeSeries.of( firstMetadata, expectedEvents );

        assertEquals( expected, actual );
    }

    @Test
    void testFilterByDuration()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();
        Pool.Builder<TimeSeries<Pair<Double, Double>>> b = new Pool.Builder<>();

        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( T1985_01_01T00_00_00Z );
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        TimeSeriesMetadata secondMetadata = getBoilerplateMetadataWithT0( T1985_01_02T00_00_00Z );
        second.add( Event.of( T1985_01_02T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        TimeSeriesMetadata thirdMetadata = getBoilerplateMetadataWithT0( T1985_01_03T00_00_00Z );
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 1.0, 1.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 2.0, 2.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );
        PoolMetadata meta = PoolMetadata.of();
        PoolMetadata baseMeta = PoolMetadata.of( true );
        //Add the time-series, with only one for baseline
        Pool<TimeSeries<Pair<Double, Double>>> ts = b.addData( TimeSeries.of( firstMetadata, first ) )
                                                     .addData( TimeSeries.of( secondMetadata, second ) )
                                                     .addData( TimeSeries.of( thirdMetadata, third ) )
                                                     .addDataForBaseline( TimeSeries.of( firstMetadata, first ) )
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
            TimeWindowOuter window =
                    TimeWindowOuter.of( MessageUtilities.getTimeWindow( duration, duration ) );
            TimeSeries<Pair<Double, Double>> events =
                    TimeSeriesSlicer.filter( ts.get().get( 0 ), window );
            for ( Event<Pair<Double, Double>> nextPair : events.getEvents() )
            {
                assertEquals( Pair.of( nextValue, nextValue ), nextPair.getValue() );
            }

            nextValue++;
        }

        //Check the regular duration of a time-series with one duration
        SortedSet<Event<Pair<Double, Double>>> fourth = new TreeSet<>();
        fourth.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 3.0, 3.0 ) ) );

        Pool.Builder<TimeSeries<Pair<Double, Double>>> bu = new Pool.Builder<>();

        Pool<TimeSeries<Pair<Double, Double>>> durationCheck =
                bu.addData( TimeSeries.of( firstMetadata, fourth ) )
                  .setMetadata( meta )
                  .build();

        TimeSeries<Pair<Double, Double>> next = durationCheck.get().get( 0 );
        next = TimeSeriesSlicer.filter( next,
                                        TimeWindowOuter.of( MessageUtilities.getTimeWindow( Duration.ofHours(
                                                                                                    51 ),
                                                                                            Duration.ofHours(
                                                                                                    51 ) ) ) );

        Duration actualDuration = Duration.between( next.getReferenceTimes().values().iterator().next(),
                                                    next.getEvents().first().getTime() );

        assertEquals( Duration.ofHours( 51 ), actualDuration );
    }

    @Test
    void testFilterByEvent()
    {

        // Create the series to filter
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( T1985_01_01T00_00_00Z );
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
    void testGroupEventsByIntervalProducesThreeGroupsEachWithTwoEvents()
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
        SortedSet<Instant> endsAt = new TreeSet<>();
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
    void testGroupEventsByOverlappingIntervalProducesTwoGroupsEachWithFourEvents()
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
        SortedSet<Instant> endsAt = new TreeSet<>();
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
    void testDecomposeWithoutLabelsProducesFourTraces()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( baseInstant );
        TimeSeries<Ensemble> ensemble =
                new TimeSeries.Builder<Ensemble>()
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
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 1.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 5.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 9.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 13.0 ) )
                        .setMetadata( metadata )
                        .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                        .setMetadata( metadata )
                        .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                        .setMetadata( metadata )
                        .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeries.Builder<Double>()
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
    void testDecomposeWithLabelsProducesFourTraces()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;

        Labels labels = Labels.of( "a", "b", "c", "d" );
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( baseInstant );
        TimeSeries<Ensemble> ensemble =
                new TimeSeries.Builder<Ensemble>()
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
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 1.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 5.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 9.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 13.0 ) )
                        .setMetadata( metadata )
                        .build();

        expected.add( one );

        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 2.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 6.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 10.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 14.0 ) )
                        .setMetadata( metadata )
                        .build();

        expected.add( two );

        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 1 ) ), 3.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 2 ) ), 7.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 3 ) ), 11.0 ) )
                        .addEvent( Event.of( baseInstant.plus( Duration.ofHours( 4 ) ), 15.0 ) )
                        .setMetadata( metadata )
                        .build();

        expected.add( three );

        TimeSeries<Double> four =
                new TimeSeries.Builder<Double>()
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
    void testDecomposeAndThenComposeWithoutLabelsProducesTheSameSeries()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;
        TimeSeriesMetadata expectedMetadata =
                getBoilerplateMetadataWithT0( baseInstant );
        TimeSeries<Ensemble> expected =
                new TimeSeries.Builder<Ensemble>()
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
    void testDecomposeAndThenComposeWithLabelsProducesTheSameSeries()
    {
        // Create an ensemble time-series with four members
        Instant baseInstant = T2086_05_01T00_00_00Z;

        Labels labels = Labels.of( "a", "b", "c", "d" );
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( baseInstant );
        TimeSeries<Ensemble> expected =
                new TimeSeries.Builder<Ensemble>()
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
    void testFilterTimeSeriesOfSingleValuedPairs()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();
        Pool.Builder<TimeSeries<Pair<Double, Double>>> b = new Pool.Builder<>();

        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( T1985_01_01T00_00_00Z );
        first.add( Event.of( T1985_01_01T01_00_00Z, Pair.of( 1.0, 10.0 ) ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, Pair.of( 2.0, 11.0 ) ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, Pair.of( 3.0, 12.0 ) ) );

        TimeSeriesMetadata secondMetadata = getBoilerplateMetadataWithT0( T1985_01_02T00_00_00Z );
        second.add( Event.of( T1985_01_02T01_00_00Z,
                              Pair.of( 4.0, 13.0 ) ) );
        second.add( Event.of( T1985_01_02T02_00_00Z,
                              Pair.of( 5.0, 14.0 ) ) );
        second.add( Event.of( T1985_01_02T03_00_00Z,
                              Pair.of( 6.0, 15.0 ) ) );

        TimeSeriesMetadata thirdMetadata = getBoilerplateMetadataWithT0( T1985_01_03T00_00_00Z );
        third.add( Event.of( T1985_01_03T01_00_00Z, Pair.of( 7.0, 16.0 ) ) );
        third.add( Event.of( T1985_01_03T02_00_00Z, Pair.of( 8.0, 17.0 ) ) );
        third.add( Event.of( T1985_01_03T03_00_00Z, Pair.of( 9.0, 18.0 ) ) );

        Geometry geometry = Geometry.newBuilder()
                                    .setName( "feature" )
                                    .build();
        Feature feature = Feature.of( geometry );

        PoolMetadata meta =
                PoolMetadata.of( Evaluation.newBuilder()
                                           .setMeasurementUnit( "foo" )
                                           .build(),
                                 wres.statistics.generated.Pool.newBuilder()
                                                               .setGeometryGroup( GeometryGroup.newBuilder()
                                                                                               .addGeometryTuples(
                                                                                                       GeometryTuple.newBuilder()
                                                                                                                    .setLeft(
                                                                                                                            geometry )
                                                                                                                    .setRight(
                                                                                                                            geometry ) ) )
                                                               .build() );

        //Add the time-series
        Pool<TimeSeries<Pair<Double, Double>>> firstSeries = b.addData( TimeSeries.of( firstMetadata,
                                                                                       first ) )
                                                              .addData( TimeSeries.of( secondMetadata,
                                                                                       second ) )
                                                              .addData( TimeSeries.of( thirdMetadata,
                                                                                       third ) )
                                                              .setMetadata( meta )
                                                              .build();

        // Filter all values where the left side is greater than 0
        Pool<TimeSeries<Pair<Double, Double>>> firstResult =
                TimeSeriesSlicer.filter( firstSeries,
                                         Slicer.left( value -> value > 0 ),
                                         null );

        assertEquals( firstSeries, firstResult );

        // Filter all values where the left side is greater than 3
        Pool<TimeSeries<Pair<Double, Double>>> secondResult =
                TimeSeriesSlicer.filter( firstSeries,
                                         Slicer.left( value -> value > 3 ),
                                         clim -> clim > 0 );

        List<Event<Pair<Double, Double>>> secondData = new ArrayList<>();
        secondResult.get().forEach( nextSeries -> secondData.addAll( nextSeries.getEvents() ) );
        List<Event<Pair<Double, Double>>> secondBenchmark = new ArrayList<>();
        secondBenchmark.addAll( second );
        secondBenchmark.addAll( third );

        assertEquals( secondBenchmark, secondData );

        // Add climatology for later
        Climatology climatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3, 4, 5, Double.NaN }, "bar" )
                                         .build();
        Climatology climatologyExpected =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3, 4, 5 }, "bar" )
                                         .build();

        b.setClimatology( climatology );

        // Filter all values where the left and right sides are both greater than or equal to 7
        Pool<TimeSeries<Pair<Double, Double>>> thirdResult =
                TimeSeriesSlicer.filter( firstSeries,
                                         Slicer.leftAndRight( value -> value >= 7 ),
                                         null );

        List<Event<Pair<Double, Double>>> thirdData = new ArrayList<>();
        thirdResult.get().forEach( nextSeries -> thirdData.addAll( nextSeries.getEvents() ) );
        List<Event<Pair<Double, Double>>> thirdBenchmark = new ArrayList<>( third );

        assertEquals( thirdBenchmark, thirdData );

        // Filter on climatology simultaneously
        Pool<TimeSeries<Pair<Double, Double>>> fourthResult =
                TimeSeriesSlicer.filter( b.build(),
                                         Slicer.leftAndRight( value -> value > 7 ),
                                         Double::isFinite );

        assertEquals( climatologyExpected, fourthResult.getClimatology() );

        // Also filter baseline data
        PoolMetadata baseMeta = PoolMetadata.of( true );
        b.addDataForBaseline( TimeSeries.of( firstMetadata, first ) )
         .addDataForBaseline( TimeSeries.of( secondMetadata, second ) )
         .setMetadataForBaseline( baseMeta );

        // Filter all values where both sides are greater than or equal to 4
        Pool<TimeSeries<Pair<Double, Double>>> fifthResult =
                TimeSeriesSlicer.filter( b.build(),
                                         Slicer.left( value -> value >= 4 ),
                                         clim -> clim > 0 );

        List<Event<Pair<Double, Double>>> fifthData = new ArrayList<>();
        fifthResult.get().forEach( nextSeries -> fifthData.addAll( nextSeries.getEvents() ) );

        // Same as second benchmark for main data
        assertEquals( secondBenchmark, fifthData );

        // Baseline data
        List<Event<Pair<Double, Double>>> fifthDataBase = new ArrayList<>();
        fifthResult.getBaselineData()
                   .get()
                   .forEach( nextSeries -> fifthDataBase.addAll( nextSeries.getEvents() ) );
        List<Event<Pair<Double, Double>>> fifthBenchmarkBase = new ArrayList<>( second );

        assertEquals( fifthBenchmarkBase, fifthDataBase );
    }

    @Test
    void testFilterTimeSeriesByEvent()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Double>> first = new TreeSet<>();

        TimeSeriesMetadata firstMetadata = getBoilerplateMetadataWithT0( T1985_01_01T00_00_00Z );
        first.add( Event.of( T1985_01_01T01_00_00Z, 1.0 ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, 2.0 ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, 3.0 ) );

        //Add the time-series
        TimeSeries<Double> series = TimeSeries.of( firstMetadata, first );

        // Filter all values where the valid time is not 1985-01-01T02:00:00Z
        TimeSeries<Double> actual = TimeSeriesSlicer.filterByEvent( series,
                                                                    next -> next.getTime()
                                                                                .equals( T1985_01_01T02_00_00Z ) );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().addEvent( Event.of( T1985_01_01T02_00_00Z, 2.0 ) )
                                                .setMetadata( firstMetadata )
                                                .build();

        assertEquals( expected, actual );
    }

    @Test
    void testSnipWithBufferOnLowerBoundAndUpperBound()
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
    void testApplyTimeOffsetToValidTimes()
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
    void testMapEventsByDuration()
    {
        // Build a time-series to adjust
        SortedSet<Event<Double>> first = new TreeSet<>();
        first.add( Event.of( T1985_01_01T01_00_00Z, 1.0 ) );
        first.add( Event.of( T1985_01_01T02_00_00Z, 2.0 ) );
        first.add( Event.of( T1985_01_01T03_00_00Z, 3.0 ) );

        //Add the time-series
        TimeSeriesMetadata metadata =
                getBoilerplateMetadataWithT0( T1985_01_01T00_00_00Z );
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
    void testGetMidpointBetweenTwoTimes()
    {
        Instant actual = TimeSeriesSlicer.getMidPointBetweenTimes( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                   Instant.parse( "1985-01-10T00:00:00Z" ) );

        Instant expected = Instant.parse( "1985-01-05T12:00:00Z" );

        assertEquals( expected, actual, "Unexpected error in mid-point of time window." );
    }

    @Test
    void testGetIntervalsFromTimeScaleWithBothMonthDaysPresent()
    {
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setStartDay( 1 )
                                       .setStartMonth( 4 )
                                       .setEndDay( 31 )
                                       .setEndMonth( 7 )
                                       .build();

        TimeScaleOuter outerTimeScale = TimeScaleOuter.of( timeScale );

        // Create a time-series that contains three years
        TimeSeries<String> fooSeries =
                new TimeSeries.Builder<String>().addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ),
                                                                     "a" ) )
                                                .addEvent( Event.of( Instant.parse( "1989-10-05T00:00:00Z" ),
                                                                     "b" ) )
                                                .addEvent( Event.of( Instant.parse( "1990-10-05T00:00:00Z" ),
                                                                     "c" ) )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                                .build();

        SortedSet<Pair<Instant, Instant>> actual =
                TimeSeriesSlicer.getIntervalsFromTimeScaleWithMonthDays( outerTimeScale,
                                                                         fooSeries );

        SortedSet<Pair<Instant, Instant>> expected = new TreeSet<>();
        expected.add( Pair.of( Instant.parse( "1988-03-31T23:59:59.999999999Z" ),
                               Instant.parse( "1988-07-31T23:59:59.999999999Z" ) ) );
        expected.add( Pair.of( Instant.parse( "1989-03-31T23:59:59.999999999Z" ),
                               Instant.parse( "1989-07-31T23:59:59.999999999Z" ) ) );
        expected.add( Pair.of( Instant.parse( "1990-03-31T23:59:59.999999999Z" ),
                               Instant.parse( "1990-07-31T23:59:59.999999999Z" ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetIntervalsFromTimeScaleWithBothMonthDaysPresentAndYearEndWrapping()
    {
        // Interval that wraps a year end
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setStartDay( 31 )
                                       .setStartMonth( 12 )
                                       .setEndDay( 1 )
                                       .setEndMonth( 1 )
                                       .build();

        TimeScaleOuter outerTimeScale = TimeScaleOuter.of( timeScale );

        // Create a time-series that contains three times
        TimeSeries<String> fooSeries =
                new TimeSeries.Builder<String>().addEvent( Event.of( Instant.parse( "1988-12-31T18:00:00Z" ),
                                                                     "a" ) )
                                                .addEvent( Event.of( Instant.parse( "1989-01-01T00:00:00Z" ),
                                                                     "b" ) )
                                                .addEvent( Event.of( Instant.parse( "1989-01-01T06:00:00Z" ),
                                                                     "c" ) )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                                .build();

        SortedSet<Pair<Instant, Instant>> actual =
                TimeSeriesSlicer.getIntervalsFromTimeScaleWithMonthDays( outerTimeScale,
                                                                         fooSeries );

        SortedSet<Pair<Instant, Instant>> expected = new TreeSet<>();
        expected.add( Pair.of( Instant.parse( "1988-12-30T23:59:59.999999999Z" ),
                               Instant.parse( "1989-01-01T23:59:59.999999999Z" ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetIntervalFromMonthDays()
    {
        Function<Instant, Pair<Instant, Instant>> calculator =
                TimeSeriesSlicer.getIntervalFromMonthDays( MonthDay.of( 10, 1 ),
                                                           MonthDay.of( 3, 31 ) );


        Pair<Instant, Instant> one = calculator.apply( Instant.parse( "1985-12-05T12:00:00Z" ) ); // Inside before YE
        Pair<Instant, Instant> two = calculator.apply( Instant.parse( "1985-01-21T12:00:00Z" ) ); // Inside after YE
        Pair<Instant, Instant> three = calculator.apply( Instant.parse( "1985-09-05T12:00:00Z" ) ); // Outside

        Pair<Instant, Instant> expectedOne = Pair.of( Instant.parse( "1985-09-30T23:59:59.999999999Z" ),
                                                      Instant.parse( "1986-03-31T23:59:59.999999999Z" ) );
        Pair<Instant, Instant> expectedTwo = Pair.of( Instant.parse( "1984-09-30T23:59:59.999999999Z" ),
                                                      Instant.parse( "1985-03-31T23:59:59.999999999Z" ) );
        Pair<Instant, Instant> expectedThree = Pair.of( Instant.parse( "1984-09-30T23:59:59.999999999Z" ),
                                                        Instant.parse( "1985-03-31T23:59:59.999999999Z" ) );


        assertEquals( expectedOne, one );
        assertEquals( expectedTwo, two );
        assertEquals( expectedThree, three );
    }

    @Test
    void testGetIntervalFromMonthDaysStartsOnEndMonthDay()
    {
        Function<Instant, Pair<Instant, Instant>> calculator =
                TimeSeriesSlicer.getIntervalFromMonthDays( MonthDay.of( 10, 1 ),
                                                           MonthDay.of( 3, 31 ) );


        Pair<Instant, Instant> one = calculator.apply( Instant.parse( "1985-10-01T12:00:00Z" ) );

        Pair<Instant, Instant> expectedOne = Pair.of( Instant.parse( "1985-09-30T23:59:59.999999999Z" ),
                                                      Instant.parse( "1986-03-31T23:59:59.999999999Z" ) );

        assertEquals( expectedOne, one );
    }

    @Test
    void testGetIntervalsFromTimeScaleWithStartMonthDayAndPeriodPresent()
    {
        // 90 days from 1 April
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setStartDay( 1 )
                                       .setStartMonth( 4 )
                                       .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                               .setSeconds( 60 * 60 * 24 * 90 ) )
                                       .build();

        TimeScaleOuter outerTimeScale = TimeScaleOuter.of( timeScale );

        // Create a time-series that contains three years
        TimeSeries<String> fooSeries =
                new TimeSeries.Builder<String>().addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ),
                                                                     "a" ) )
                                                .addEvent( Event.of( Instant.parse( "1989-10-05T00:00:00Z" ),
                                                                     "b" ) )
                                                .addEvent( Event.of( Instant.parse( "1990-10-05T00:00:00Z" ),
                                                                     "c" ) )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                                .build();

        SortedSet<Pair<Instant, Instant>> actual =
                TimeSeriesSlicer.getIntervalsFromTimeScaleWithMonthDays( outerTimeScale,
                                                                         fooSeries );

        SortedSet<Pair<Instant, Instant>> expected = new TreeSet<>();
        expected.add( Pair.of( Instant.parse( "1988-03-31T23:59:59.999999999Z" ),
                               Instant.parse( "1988-06-29T23:59:59.999999999Z" ) ) );
        expected.add( Pair.of( Instant.parse( "1989-03-31T23:59:59.999999999Z" ),
                               Instant.parse( "1989-06-29T23:59:59.999999999Z" ) ) );
        expected.add( Pair.of( Instant.parse( "1990-03-31T23:59:59.999999999Z" ),
                               Instant.parse( "1990-06-29T23:59:59.999999999Z" ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetRegularSequenceOfIntersectingTimesForOneTimeWindow()
    {
        SortedSet<Event<Double>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 2.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 3.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-05T03:00:00Z" ), 4.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ), 5.0 ) );

        TimeSeries<Double> left =
                new TimeSeries.Builder<Double>().addEvents( leftEvents )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                                .build();

        Instant referenceTime = Instant.parse( "1988-10-04T06:00:00Z" );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( Instant.parse( "1988-10-04T12:00:00Z" ), 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 3.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ), 4.0 ) );

        TimeSeries<Double> right =
                new TimeSeries.Builder<Double>().addEvents( rightEvents )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadataWithT0(
                                                        referenceTime ) )
                                                .build();

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( Duration.ofHours( 6 ) );

        TimeWindowOuter timeWindow = TimeWindowOuter.of( MessageUtilities.getTimeWindow() );

        Duration frequency = Duration.ofHours( 6 );

        SortedSet<Instant> actual = TimeSeriesSlicer.getRegularSequenceOfIntersectingTimes( left,
                                                                                            right,
                                                                                            timeWindow,
                                                                                            desiredTimeScale,
                                                                                            frequency );

        SortedSet<Instant> expected = new TreeSet<>();
        expected.add( Instant.parse( "1988-10-04T18:00:00Z" ) );
        expected.add( Instant.parse( "1988-10-05T00:00:00Z" ) );
        expected.add( Instant.parse( "1988-10-05T06:00:00Z" ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetRegularSequenceOfIntersectingTimesForOneTimeWindowAndFrequencySmallerThanPeriod()
    {
        SortedSet<Event<Double>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 2.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 3.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-05T03:00:00Z" ), 4.0 ) );
        leftEvents.add( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ), 5.0 ) );

        TimeSeries<Double> left =
                new TimeSeries.Builder<Double>().addEvents( leftEvents )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                                .build();

        Instant referenceTime = Instant.parse( "1988-10-04T06:00:00Z" );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( Instant.parse( "1988-10-04T12:00:00Z" ), 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-04T15:00:00Z" ), 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 2.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 3.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-05T03:00:00Z" ), 1.0 ) );
        rightEvents.add( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ), 4.0 ) );

        TimeSeries<Double> right =
                new TimeSeries.Builder<Double>().addEvents( rightEvents )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadataWithT0(
                                                        referenceTime ) )
                                                .build();

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( Duration.ofHours( 6 ) );

        TimeWindowOuter timeWindow = TimeWindowOuter.of( MessageUtilities.getTimeWindow() );

        Duration frequency = Duration.ofHours( 3 );

        SortedSet<Instant> actual = TimeSeriesSlicer.getRegularSequenceOfIntersectingTimes( left,
                                                                                            right,
                                                                                            timeWindow,
                                                                                            desiredTimeScale,
                                                                                            frequency );

        SortedSet<Instant> expected = new TreeSet<>();
        expected.add( Instant.parse( "1988-10-04T18:00:00Z" ) );
        expected.add( Instant.parse( "1988-10-04T21:00:00Z" ) );
        expected.add( Instant.parse( "1988-10-05T00:00:00Z" ) );
        expected.add( Instant.parse( "1988-10-05T03:00:00Z" ) );
        expected.add( Instant.parse( "1988-10-05T06:00:00Z" ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetRegularSequenceOfIntersectingTimesForTwoTimeWindows()
    {
        Instant referenceTime = Instant.parse( "1988-10-04T12:00:00Z" );

        SortedSet<Event<Double>> events = new TreeSet<>();
        events.add( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 2.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-05T06:00:00Z" ), 3.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-05T12:00:00Z" ), 4.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-05T18:00:00Z" ), 5.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-06T00:00:00Z" ), 6.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-06T06:00:00Z" ), 7.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-06T12:00:00Z" ), 8.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-06T18:00:00Z" ), 9.0 ) );
        events.add( Event.of( Instant.parse( "1988-10-07T00:00:00Z" ), 10.0 ) );

        TimeSeries<Double> left =
                new TimeSeries.Builder<Double>().addEvents( events )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                                .build();

        TimeSeries<Double> right =
                new TimeSeries.Builder<Double>().addEvents( events )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadataWithT0(
                                                        referenceTime ) )
                                                .build();

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( Duration.ofDays( 1 ) );

        TimeWindowOuter timeWindow =
                TimeWindowOuter.of( MessageUtilities.getTimeWindow( Duration.ZERO,
                                                                    Duration.ofDays( 1 ) ) );

        Duration frequency = Duration.ofDays( 1 );

        SortedSet<Instant> actualOne = TimeSeriesSlicer.getRegularSequenceOfIntersectingTimes( left,
                                                                                               right,
                                                                                               timeWindow,
                                                                                               desiredTimeScale,
                                                                                               frequency );

        SortedSet<Instant> expectedOne = new TreeSet<>();
        expectedOne.add( Instant.parse( "1988-10-05T12:00:00Z" ) );

        assertEquals( expectedOne, actualOne );

        TimeWindowOuter timeWindowTwo =
                TimeWindowOuter.of( MessageUtilities.getTimeWindow( Duration.ofDays( 1 ),
                                                                    Duration.ofDays( 2 ) ) );


        SortedSet<Instant> actualTwo = TimeSeriesSlicer.getRegularSequenceOfIntersectingTimes( left,
                                                                                               right,
                                                                                               timeWindowTwo,
                                                                                               desiredTimeScale,
                                                                                               frequency );
        SortedSet<Instant> expectedTwo = new TreeSet<>();
        expectedTwo.add( Instant.parse( "1988-10-06T12:00:00Z" ) );

        assertEquals( expectedTwo, actualTwo );
    }

    @Test
    void testGetRegularSequenceOfIntersectingTimesForOneTimeWindowLargePeriod()
    {
        // Akin to system test scenario801, but with 6-hourly observations

        // CKLN6_STG.xml
        SortedSet<Event<Double>> leftEvents = new TreeSet<>();
        leftEvents.add( Event.of( Instant.parse( "2017-01-04T12:00:00Z" ), 5.56 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-04T18:00:00Z" ), 6.06 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-05T00:00:00Z" ), 6.45 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-05T06:00:00Z" ), 6.68 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-05T12:00:00Z" ), 6.94 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-05T18:00:00Z" ), 7.01 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-06T00:00:00Z" ), 6.92 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-06T06:00:00Z" ), 6.76 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-06T12:00:00Z" ), 6.54 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-06T18:00:00Z" ), 6.29 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-07T00:00:00Z" ), 6.09 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-07T06:00:00Z" ), 5.89 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-07T12:00:00Z" ), 5.65 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-07T18:00:00Z" ), 5.34 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-08T00:00:00Z" ), 5.2 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-08T06:00:00Z" ), 5.3 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-08T12:00:00Z" ), 5.25 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-08T18:00:00Z" ), 5.02 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-09T00:00:00Z" ), 4.62 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-09T06:00:00Z" ), 4.68 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-09T12:00:00Z" ), 4.79 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-09T18:00:00Z" ), 4.25 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-10T00:00:00Z" ), 4.43 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-10T06:00:00Z" ), 5.45 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-10T12:00:00Z" ), 4.94 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-10T18:00:00Z" ), 4.72 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-11T00:00:00Z" ), 4.64 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-11T06:00:00Z" ), 4.67 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-11T12:00:00Z" ), 4.73 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-11T18:00:00Z" ), 4.52 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-12T00:00:00Z" ), 4.81 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-12T06:00:00Z" ), 5.1 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-12T12:00:00Z" ), 5.18 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-12T18:00:00Z" ), 5.4 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-13T00:00:00Z" ), 6.15 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-13T06:00:00Z" ), 6.91 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-13T12:00:00Z" ), 8.48 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-13T18:00:00Z" ), 8.37 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-14T00:00:00Z" ), 8.59 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-14T06:00:00Z" ), 8.64 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-14T12:00:00Z" ), 8.6 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-14T18:00:00Z" ), 8.39 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-15T00:00:00Z" ), 8.02 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-15T06:00:00Z" ), 7.54 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-15T12:00:00Z" ), 7.09 ) );
        leftEvents.add( Event.of( Instant.parse( "2017-01-15T18:00:00Z" ), 6.78 ) );

        TimeSeries<Double> left =
                new TimeSeries.Builder<Double>().addEvents( leftEvents )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                                                .build();

        // 2017010512_HEFS_export.xml
        Instant referenceTime = Instant.parse( "2017-01-05T12:00:00Z" );

        SortedSet<Event<Double>> rightEvents = new TreeSet<>();
        rightEvents.add( Event.of( Instant.parse( "2017-01-04T12:00:00Z" ), 5.8802495 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-04T18:00:00Z" ), 6.33563 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-05T00:00:00Z" ), 6.7591863 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-05T06:00:00Z" ), 6.977034 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-05T12:00:00Z" ), 7.1925855 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-05T18:00:00Z" ), 7.2486877 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-06T00:00:00Z" ), 7.246063 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-06T06:00:00Z" ), 7.2526245 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-06T12:00:00Z" ), 7.318242 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-06T18:00:00Z" ), 7.491798 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-07T00:00:00Z" ), 7.586942 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-07T06:00:00Z" ), 7.5095143 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-07T12:00:00Z" ), 7.3517056 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-07T18:00:00Z" ), 7.1551843 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-08T00:00:00Z" ), 6.9317584 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-08T06:00:00Z" ), 6.6955385 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-08T12:00:00Z" ), 6.477034 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-08T18:00:00Z" ), 6.290026 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-09T00:00:00Z" ), 6.11811 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-09T06:00:00Z" ), 5.963911 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-09T12:00:00Z" ), 5.836942 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-09T18:00:00Z" ), 5.720801 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-10T00:00:00Z" ), 5.612861 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-10T06:00:00Z" ), 5.507218 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-10T12:00:00Z" ), 5.4028873 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-10T18:00:00Z" ), 5.714895 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-11T00:00:00Z" ), 6.7982283 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-11T06:00:00Z" ), 7.7752624 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-11T12:00:00Z" ), 8.2732935 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-11T18:00:00Z" ), 8.717192 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-12T00:00:00Z" ), 9.030184 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-12T06:00:00Z" ), 9.397637 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-12T12:00:00Z" ), 9.540026 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-12T18:00:00Z" ), 9.365814 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-13T00:00:00Z" ), 9.079068 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-13T06:00:00Z" ), 8.835629 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-13T12:00:00Z" ), 8.660762 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-13T18:00:00Z" ), 8.644686 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-14T00:00:00Z" ), 8.871718 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-14T06:00:00Z" ), 9.053805 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-14T12:00:00Z" ), 9.056759 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-14T18:00:00Z" ), 9.064304 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-15T00:00:00Z" ), 9.055446 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-15T06:00:00Z" ), 9.0544615 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-15T12:00:00Z" ), 8.967192 ) );
        rightEvents.add( Event.of( Instant.parse( "2017-01-15T18:00:00Z" ), 8.737533 ) );

        TimeSeries<Double> right =
                new TimeSeries.Builder<Double>().addEvents( rightEvents )
                                                .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadataWithT0(
                                                        referenceTime ) )
                                                .build();

        Duration period = Duration.ofHours( 96 );
        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( period );

        // Time window from 144 hours to 240 hours
        Duration lowerLeadBound = Duration.ZERO;
        Duration upperLeadBound = Duration.ofHours( 240 );

        TimeWindowOuter timeWindow = TimeWindowOuter.of( MessageUtilities.getTimeWindow( lowerLeadBound,
                                                                                         upperLeadBound ) );

        SortedSet<Instant> actual = TimeSeriesSlicer.getRegularSequenceOfIntersectingTimes( left,
                                                                                            right,
                                                                                            timeWindow,
                                                                                            desiredTimeScale,
                                                                                            period );

        SortedSet<Instant> expected = new TreeSet<>();
        expected.add( Instant.parse( "2017-01-09T12:00:00Z" ) );
        expected.add( Instant.parse( "2017-01-13T12:00:00Z" ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetReferenceTimeSeasonFilterAcceptsExpectedTimeSeries()
    {
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                        T1985_01_01T00_00_00Z ),
                                                                TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                                                STREAMFLOW,
                                                                DRRC2,
                                                                CFS );
        // Add the time-series
        TimeSeries<Pair<Double, Double>> one = TimeSeries.of( metadataOne, new TreeSet<>() );

        MonthDay startFirst = MonthDay.of( 12, 30 );
        MonthDay endFirst = MonthDay.of( 1, 3 );

        Predicate<TimeSeries<Pair<Double, Double>>> filter =
                TimeSeriesSlicer.getReferenceTimeSeasonFilter( startFirst, endFirst );

        MonthDay startSecond = MonthDay.of( 1, 2 );
        MonthDay endSecond = MonthDay.of( 1, 3 );

        Predicate<TimeSeries<Pair<Double, Double>>> anotherFilter =
                TimeSeriesSlicer.getReferenceTimeSeasonFilter( startSecond,
                                                               endSecond );

        Assertions.assertAll( () -> assertTrue( filter.test( one ) ),
                              () -> assertFalse( anotherFilter.test( one ) ) );
    }

    @Test
    void testGetValidTimeSeasonTransformerProducesExpectedTimeSeries()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                     T1985_01_01T00_00_00Z ),
                                                             TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                                             STREAMFLOW,
                                                             DRRC2,
                                                             CFS );
        // Add the time-series
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( Instant.parse( "2029-03-01T12:00:00Z" ), 1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2029-04-01T12:00:00Z" ), 2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2029-05-01T12:00:00Z" ), 3.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2029-06-01T12:00:00Z" ), 4.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2029-07-01T12:00:00Z" ), 5.0 ) )
                                                .build();

        MonthDay startFirst = MonthDay.of( 4, 2 );
        MonthDay endFirst = MonthDay.of( 6, 2 );

        UnaryOperator<TimeSeries<Double>> transformer =
                TimeSeriesSlicer.getValidTimeSeasonTransformer( startFirst, endFirst );

        MonthDay startSecond = MonthDay.of( 11, 10 );
        MonthDay endSecond = MonthDay.of( 2, 28 );

        UnaryOperator<TimeSeries<Double>> anotherFilter =
                TimeSeriesSlicer.getValidTimeSeasonTransformer( startSecond, endSecond );

        TimeSeries<Double> expectedOne =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( Instant.parse( "2029-05-01T12:00:00Z" ), 3.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2029-06-01T12:00:00Z" ), 4.0 ) )
                                                .build();

        TimeSeries<Double> expectedTwo = new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                                         .build();

        Assertions.assertAll( () -> assertEquals( expectedOne, transformer.apply( one ) ),
                              () -> assertEquals( expectedTwo, anotherFilter.apply( one ) ) );
    }

    @Test
    void testGetIgnoredValidDatesTransformerFiltersExpectedDates()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                     T1985_01_01T00_00_00Z ),
                                                             TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                                             STREAMFLOW,
                                                             DRRC2,
                                                             CFS );

        TimeSeries<Double> series =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T1985_01_01T01_00_00Z, 3.0 ) )
                                                .addEvent( Event.of( T1985_01_01T02_00_00Z, 4.0 ) )
                                                .addEvent( Event.of( T1985_01_01T03_00_00Z, 5.0 ) )
                                                .build();

        TimeInterval firstInterval = new TimeInterval( Instant.parse( "1984-05-01T12:00:00Z" ),
                                                       T1985_01_01T01_00_00Z );
        TimeInterval secondInterval = new TimeInterval( T1985_01_01T03_00_00Z,
                                                        Instant.parse( "1986-05-01T12:00:00Z" ) );

        Set<TimeInterval> ignoredValidDates = Set.of( firstInterval, secondInterval );

        UnaryOperator<TimeSeries<Double>> transformer =
                TimeSeriesSlicer.getIgnoredValidDatesTransformer( ignoredValidDates );

        TimeSeries<Double> actual = transformer.apply( series );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T1985_01_01T02_00_00Z, 4.0 ) )
                                                .build();
        assertEquals( expected, actual );
    }

    @Test
    void testGetIgnoredValidDatesTransformerFiltersNoDates()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                     T1985_01_01T00_00_00Z ),
                                                             TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                                             STREAMFLOW,
                                                             DRRC2,
                                                             CFS );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T1985_01_01T01_00_00Z, 3.0 ) )
                                                .addEvent( Event.of( T1985_01_01T02_00_00Z, 4.0 ) )
                                                .addEvent( Event.of( T1985_01_01T03_00_00Z, 5.0 ) )
                                                .build();

        TimeInterval firstInterval = new TimeInterval( Instant.parse( "1986-05-01T12:00:00Z" ),
                                                       Instant.parse( "1987-05-01T12:00:00Z" ) );

        Set<TimeInterval> ignoredValidDates = Set.of( firstInterval );

        UnaryOperator<TimeSeries<Double>> transformer =
                TimeSeriesSlicer.getIgnoredValidDatesTransformer( ignoredValidDates );

        TimeSeries<Double> actual = transformer.apply( expected );

        assertEquals( expected, actual );
    }

    @Test
    void testGetIgnoredValidDatesTransformerFiltersNoDatesWhenNoIntervalsProvided()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                     T1985_01_01T00_00_00Z ),
                                                             TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                                             STREAMFLOW,
                                                             DRRC2,
                                                             CFS );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T1985_01_01T01_00_00Z, 3.0 ) )
                                                .addEvent( Event.of( T1985_01_01T02_00_00Z, 4.0 ) )
                                                .addEvent( Event.of( T1985_01_01T03_00_00Z, 5.0 ) )
                                                .build();

        UnaryOperator<TimeSeries<Double>> transformer =
                TimeSeriesSlicer.getIgnoredValidDatesTransformer( Collections.emptySet() );

        TimeSeries<Double> actual = transformer.apply( expected );

        assertEquals( expected, actual );
    }

    @Test
    void testGetValidTimes()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                     T1985_01_01T00_00_00Z ),
                                                             TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                                             STREAMFLOW,
                                                             DRRC2,
                                                             CFS );

        TimeSeries<Double> series =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T1985_01_01T01_00_00Z, 3.0 ) )
                                                .addEvent( Event.of( T1985_01_01T02_00_00Z, 4.0 ) )
                                                .build();

        SortedSet<Instant> actual = TimeSeriesSlicer.getValidTimes( series );

        SortedSet<Instant> expected = new TreeSet<>();
        expected.add( T1985_01_01T01_00_00Z );
        expected.add( T1985_01_01T02_00_00Z );

        assertEquals( expected, actual );
    }

    @Test
    void testGetEnsembleTransformer()
    {
        UnaryOperator<Double> multiplyByThree = in -> in * 3.0;
        UnaryOperator<Ensemble> multiplyEnsembleByThree = TimeSeriesSlicer.getEnsembleTransformer( multiplyByThree );
        Ensemble one = Ensemble.of( 1.0 );
        Ensemble actual = multiplyEnsembleByThree.apply( one );
        Ensemble expected = Ensemble.of( 3.0 );
        assertEquals( expected, actual );
    }

    @Test
    void testGetTimesteps()
    {
        TimeSeries<Double> series =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T21:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T00:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-05T04:00:00Z" ), 1.0 ) )
                        .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                        .build();

        SortedSet<Duration> actual = TimeSeriesSlicer.getTimesteps( series );
        SortedSet<Duration> expected = new TreeSet<>( Set.of( Duration.ofHours( 1 ),
                                                              Duration.ofHours( 2 ),
                                                              Duration.ofHours( 3 ),
                                                              Duration.ofHours( 4 ) ) );
        assertEquals( expected, actual );
    }

    @Test
    void testGroupByEventCount()
    {
        TimeSeries<Double> one =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                        .build();
        TimeSeries<Double> two =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) )
                        .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                        .build();
        TimeSeries<Double> three =
                new TimeSeries.Builder<Double>()
                        .addEvent( Event.of( Instant.parse( "1988-10-04T17:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T18:00:00Z" ), 1.0 ) )
                        .addEvent( Event.of( Instant.parse( "1988-10-04T19:00:00Z" ), 1.0 ) )
                        .setMetadata( TimeSeriesSlicerTest.getBoilerplateMetadata() )
                        .build();
        List<TimeSeries<Double>> series = List.of( one, two, two, three, three, three );

        Map<Integer, List<TimeSeries<Double>>> actual = TimeSeriesSlicer.groupByEventCount( series );
        Map<Integer, List<TimeSeries<Double>>> expected = Map.of( 1, List.of( one ),
                                                                  2, List.of( two, two ),
                                                                  3, List.of( three, three, three ) );
        assertEquals( expected, actual );
    }

    /**
     * Gets some boilerplate metadata with a reference time.
     * @param t0 the reference time
     * @return the metadata
     */
    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0 )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

    /**
     * @return some boilerplate metadata
     */

    private static TimeSeriesMetadata getBoilerplateMetadata()
    {
        return TimeSeriesMetadata.of( Map.of(),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }
}

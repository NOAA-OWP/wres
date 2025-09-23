package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.LeadTimeInterval;
import wres.config.components.LeadTimeIntervalBuilder;
import wres.config.components.TimeInterval;
import wres.config.components.TimeIntervalBuilder;
import wres.config.components.TimePools;
import wres.config.components.TimePoolsBuilder;
import wres.config.components.TimeWindowAggregation;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link TimeWindowSlicer}.
 */
class TimeWindowSlicerTest
{
    private static final Instant FOURTH_TIME = Instant.parse( "2017-12-31T11:59:59Z" );
    private static final Instant THIRD_TIME = Instant.parse( "2015-12-31T11:59:59Z" );
    private static final Instant SECOND_TIME = Instant.parse( "1985-01-01T00:00:00Z" );
    private static final Instant FIRST_TIME = Instant.parse( "1983-01-01T00:00:00Z" );
    private static final String INSTANT_ONE = "2017-08-08T00:00:00Z";
    private static final String INSTANT_TWO = "2017-08-08T23:00:00Z";
    private static final String INSTANT_THREE = "2017-08-09T17:00:00Z";
    private static final String INSTANT_FOUR = "2017-08-08T01:00:00Z";
    private static final String INSTANT_FIVE = "2017-08-08T02:00:00Z";
    private static final String INSTANT_SIX = "2017-08-08T03:00:00Z";
    private static final String INSTANT_SEVEN = "2017-08-08T04:00:00Z";
    private static final String INSTANT_EIGHT = "2017-08-08T05:00:00Z";
    private static final String INSTANT_NINE = "2551-03-17T00:00:00Z";
    private static final String INSTANT_TEN = "2551-03-20T00:00:00Z";
    private static final String INSTANT_ELEVEN = "2551-03-19T00:00:00Z";
    private static final String INSTANT_TWELVE = "2551-03-17T13:00:00Z";
    private static final String INSTANT_THIRTEEN = "2551-03-17T07:00:00Z";
    private static final String INSTANT_FOURTEEN = "2551-03-17T20:00:00Z";
    private static final String INSTANT_FIFTEEN = "2551-03-17T14:00:00Z";
    private static final String INSTANT_SIXTEEN = "2551-03-18T03:00:00Z";
    private static final String INSTANT_SEVENTEEN = "2551-03-17T21:00:00Z";
    private static final String INSTANT_EIGHTEEN = "2551-03-18T10:00:00Z";
    private static final String INSTANT_NINETEEN = "2551-03-18T04:00:00Z";
    private static final String INSTANT_TWENTY = "2551-03-18T17:00:00Z";
    private static final String INSTANT_TWENTY_ONE = "2551-03-18T11:00:00Z";
    private static final String INSTANT_TWENTY_TWO = "2551-03-18T18:00:00Z";
    private static final String INSTANT_TWENTY_THREE = "2551-03-19T07:00:00Z";
    private static final String INSTANT_TWENTY_FOUR = "2551-03-19T01:00:00Z";
    private static final String INSTANT_TWENTY_FIVE = "2551-03-19T14:00:00Z";
    private static final String INSTANT_TWENTY_SIX = "2551-03-19T08:00:00Z";
    private static final String INSTANT_TWENTY_SEVEN = "2551-03-19T21:00:00Z";
    private static final String INSTANT_TWENTY_EIGHT = "2551-03-24T00:00:00Z";

    /**
     * Tests the {@link TimeWindowSlicer#union(Set)}.
     */

    @Test
    void testUnion()
    {
        TimeWindow firstInner = MessageUtilities.getTimeWindow( SECOND_TIME,
                                                                FOURTH_TIME,
                                                                Duration.ofHours( 5 ),
                                                                Duration.ofHours( 25 ) );
        TimeWindowOuter first = TimeWindowOuter.of( firstInner );
        TimeWindow secondInner = MessageUtilities.getTimeWindow( FIRST_TIME,
                                                                 THIRD_TIME,
                                                                 Duration.ofHours( -5 ),
                                                                 Duration.ofHours( 20 ) );
        TimeWindowOuter second = TimeWindowOuter.of( secondInner );
        TimeWindow expectedInner = MessageUtilities.getTimeWindow( FIRST_TIME,
                                                                   FOURTH_TIME,
                                                                   Duration.ofHours( -5 ),
                                                                   Duration.ofHours( 25 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );
        Set<TimeWindowOuter> union = new HashSet<>();
        union.add( first );
        union.add( second );

        TimeWindowOuter actual = TimeWindowSlicer.union( union );

        assertEquals( expected, actual );

        TimeWindow thirdInner = MessageUtilities.getTimeWindow( SECOND_TIME,
                                                                FOURTH_TIME,
                                                                FIRST_TIME,
                                                                Instant.parse( "2019-12-31T11:59:59Z" ),
                                                                Duration.ofHours( 5 ),
                                                                Duration.ofHours( 21 ) );
        TimeWindowOuter third = TimeWindowOuter.of( thirdInner );
        TimeWindow fourthInner = MessageUtilities.getTimeWindow( FIRST_TIME,
                                                                 THIRD_TIME,
                                                                 Instant.parse( "1982-01-01T00:00:00Z" ),
                                                                 FOURTH_TIME,
                                                                 Duration.ZERO,
                                                                 Duration.ofHours( 20 ) );
        TimeWindowOuter fourth = TimeWindowOuter.of( fourthInner );
        TimeWindow fifthInner = MessageUtilities.getTimeWindow( FIRST_TIME,
                                                                FOURTH_TIME,
                                                                Instant.parse( "1982-01-01T00:00:00Z" ),
                                                                Instant.parse( "2019-12-31T11:59:59Z" ),
                                                                Duration.ZERO,
                                                                Duration.ofHours( 21 ) );
        TimeWindowOuter expectedTwo = TimeWindowOuter.of( fifthInner );
        Set<TimeWindowOuter> unionTwo = new HashSet<>();
        unionTwo.add( third );
        unionTwo.add( fourth );

        TimeWindowOuter actualTwo = TimeWindowSlicer.union( unionTwo );

        assertEquals( expectedTwo, actualTwo );
    }

    @Test
    void testUnionWithThrowsExceptionOnEmptyInput()
    {
        Set<TimeWindowOuter> emptySet = Set.of();
        IllegalArgumentException thrown =
                assertThrows( IllegalArgumentException.class,
                              () -> TimeWindowSlicer.union( emptySet ) );

        assertEquals( "Cannot determine the union of time windows for empty input.", thrown.getMessage() );
    }

    @Test
    void testUnionWithThrowsExceptionOnInputWithNull()
    {

        Set<TimeWindowOuter> nullInput = new HashSet<>();
        nullInput.add( null );

        IllegalArgumentException thrown =
                assertThrows( IllegalArgumentException.class,
                              () -> TimeWindowSlicer.union( nullInput ) );

        assertEquals( "Cannot determine the union of time windows for input that contains one or more "
                      + "null time windows.",
                      thrown.getMessage() );
    }

    @Test
    void testUnionWithThrowsExceptionOnNullInput()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class, () -> TimeWindowSlicer.union( null ) );

        assertEquals( "Cannot determine the union of time windows for a null input.", thrown.getMessage() );
    }

    @Test
    void testIntersectionOnLeadDurationsOnly()
    {
        TimeWindow one = MessageUtilities.getTimeWindow( Duration.ofHours( 1 ),
                                                         Duration.ofHours( 3 ) );
        TimeWindow two = MessageUtilities.getTimeWindow( Duration.ofHours( 3 ),
                                                         Duration.ofHours( 5 ) );
        TimeWindow three = MessageUtilities.getTimeWindow( Duration.ofHours( 1 ),
                                                           Duration.ofHours( 2 ) );
        TimeWindow four = MessageUtilities.getTimeWindow( Duration.ofHours( 6 ),
                                                          Duration.ofHours( 7 ) );
        Set<TimeWindowOuter> first = Set.of( TimeWindowOuter.of( one ),
                                             TimeWindowOuter.of( two ) );
        Set<TimeWindowOuter> second = Set.of( TimeWindowOuter.of( three ),
                                              TimeWindowOuter.of( four ) );

        Set<TimeWindowOuter> actual = TimeWindowSlicer.intersection( first, second );
        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( one ),
                                                TimeWindowOuter.of( three ) );

        assertEquals( expected, actual );
    }

    @Test
    void testIntersectionOnValidTimeOnly()
    {
        TimeWindow one = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                         Instant.parse( INSTANT_TWO ) );
        TimeWindow two = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                         Instant.parse( INSTANT_THREE ) );
        TimeWindow three = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                           Instant.parse( INSTANT_THREE ) );
        TimeWindow four = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ELEVEN ),
                                                          Instant.parse( INSTANT_TEN ) );
        Set<TimeWindowOuter> first = Set.of( TimeWindowOuter.of( one ),
                                             TimeWindowOuter.of( two ) );
        Set<TimeWindowOuter> second = Set.of( TimeWindowOuter.of( three ),
                                              TimeWindowOuter.of( four ) );

        Set<TimeWindowOuter> actual = TimeWindowSlicer.intersection( first, second );
        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( one ),
                                                TimeWindowOuter.of( two ),
                                                TimeWindowOuter.of( three ) );

        assertEquals( expected, actual );
    }

    @Test
    void testIntersectionOnValidTimeOnlyWithOddNumberOfEvents()
    {
        Instant s1 = Instant.parse( "2023-02-03T07:12:00Z" );
        Instant e1 = Instant.parse( "2023-03-09T04:48:00Z" );
        Instant s2 = Instant.parse( "2023-05-13T13:00:00Z" );
        Instant e2 = Instant.parse( "2023-06-18T10:36:00Z" );
        Instant s3 = Instant.parse( "2023-10-10T13:00:00Z" );
        Instant e3 = Instant.parse( "2023-11-15T09:36:00Z" );
        Instant s4 = Instant.parse( "2023-08-21T13:00:00Z" );
        Instant e4 = Instant.parse( "2023-09-26T10:36:00Z" );
        Instant s5 = Instant.parse( "2023-07-02T13:00:00Z" );
        Instant e5 = Instant.parse( "2023-08-07T10:36:00Z" );
        Instant s6 = Instant.parse( "2023-03-24T14:24:00Z" );
        Instant e6 = Instant.parse( "2023-04-29T10:36:00Z" );

        Instant ns1 = Instant.parse( "2023-01-16T12:00:00Z" );
        Instant ne1 = Instant.parse( "2023-02-06T16:48:00Z" );
        Instant ns2 = Instant.parse( "2023-03-04T16:48:00Z" );
        Instant ne2 = Instant.parse( "2023-03-30T05:48:00Z" );
        Instant ns3 = Instant.parse( "2023-04-23T17:48:00Z" );
        Instant ne3 = Instant.parse( "2023-05-19T05:48:00Z" );
        Instant ns4 = Instant.parse( "2023-06-12T17:48:00Z" );
        Instant ne4 = Instant.parse( "2023-07-08T05:48:00Z" );
        Instant ns5 = Instant.parse( "2023-08-01T17:48:00Z" );
        Instant ne5 = Instant.parse( "2023-08-27T05:48:00Z" );
        Instant ns6 = Instant.parse( "2023-09-20T17:48:00Z" );
        Instant ne6 = Instant.parse( "2023-10-16T05:48:00Z" );
        Instant ns7 = Instant.parse( "2023-11-09T16:48:00Z" );
        Instant ne7 = Instant.parse( "2023-12-05T04:48:00Z" );

        TimeWindow one = MessageUtilities.getTimeWindow( s1, e1 );
        TimeWindow two = MessageUtilities.getTimeWindow( s2, e2 );
        TimeWindow three = MessageUtilities.getTimeWindow( s3, e3 );
        TimeWindow four = MessageUtilities.getTimeWindow( s4, e4 );
        TimeWindow five = MessageUtilities.getTimeWindow( s5, e5 );
        TimeWindow six = MessageUtilities.getTimeWindow( s6, e6 );

        Set<TimeWindowOuter> left = Set.of( TimeWindowOuter.of( one ),
                                            TimeWindowOuter.of( two ),
                                            TimeWindowOuter.of( three ),
                                            TimeWindowOuter.of( four ),
                                            TimeWindowOuter.of( five ),
                                            TimeWindowOuter.of( six ) );

        TimeWindow oneRight = MessageUtilities.getTimeWindow( ns1, ne1 );
        TimeWindow twoRight = MessageUtilities.getTimeWindow( ns2, ne2 );
        TimeWindow threeRight = MessageUtilities.getTimeWindow( ns3, ne3 );
        TimeWindow fourRight = MessageUtilities.getTimeWindow( ns4, ne4 );
        TimeWindow fiveRight = MessageUtilities.getTimeWindow( ns5, ne5 );
        TimeWindow sixRight = MessageUtilities.getTimeWindow( ns6, ne6 );
        TimeWindow sevenRight = MessageUtilities.getTimeWindow( ns7, ne7 );

        Set<TimeWindowOuter> right = Set.of( TimeWindowOuter.of( oneRight ),
                                             TimeWindowOuter.of( twoRight ),
                                             TimeWindowOuter.of( threeRight ),
                                             TimeWindowOuter.of( fourRight ),
                                             TimeWindowOuter.of( fiveRight ),
                                             TimeWindowOuter.of( sixRight ),
                                             TimeWindowOuter.of( sevenRight ) );

        Set<TimeWindowOuter> intersection = TimeWindowSlicer.intersection( left, right );

        assertEquals( 13, intersection.size() );
    }

    @Test
    void testIntersectionOnReferenceTimeOnly()
    {
        TimeWindow one = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                         Instant.parse( INSTANT_TWO ),
                                                         Instant.MIN,
                                                         Instant.MAX );
        TimeWindow two = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                         Instant.parse( INSTANT_THREE ),
                                                         Instant.MIN,
                                                         Instant.MAX );
        TimeWindow three = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                           Instant.parse( INSTANT_THREE ),
                                                           Instant.MIN,
                                                           Instant.MAX );
        TimeWindow four = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ELEVEN ),
                                                          Instant.parse( INSTANT_TEN ),
                                                          Instant.MIN,
                                                          Instant.MAX );
        Set<TimeWindowOuter> first = Set.of( TimeWindowOuter.of( one ),
                                             TimeWindowOuter.of( two ) );
        Set<TimeWindowOuter> second = Set.of( TimeWindowOuter.of( three ),
                                              TimeWindowOuter.of( four ) );

        Set<TimeWindowOuter> actual = TimeWindowSlicer.intersection( first, second );
        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( one ),
                                                TimeWindowOuter.of( two ),
                                                TimeWindowOuter.of( three ) );

        assertEquals( expected, actual );
    }

    @Test
    void testIntersectionOnLeadDurationValidTimeAndReferenceTime()
    {
        TimeWindow one = MessageUtilities.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
                                                         Instant.parse( "1934-01-02T00:00:00Z" ),
                                                         Instant.parse( "1935-01-01T00:00:00Z" ),
                                                         Instant.parse( "1935-01-02T00:00:00Z" ),
                                                         Duration.ofHours( 1 ),
                                                         Duration.ofHours( 2 ) );
        TimeWindow two = MessageUtilities.getTimeWindow( Instant.parse( "1933-01-01T00:00:00Z" ),
                                                         Instant.parse( "1934-01-02T00:00:00Z" ),
                                                         Instant.parse( "1934-01-01T00:00:00Z" ),
                                                         Instant.parse( "1935-01-02T00:00:00Z" ),
                                                         Duration.ofHours( 1 ),
                                                         Duration.ofHours( 2 ) );
        TimeWindow three = MessageUtilities.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
                                                           Instant.parse( "1934-01-04T00:00:00Z" ),
                                                           Instant.parse( "1935-01-01T00:00:00Z" ),
                                                           Instant.parse( "1935-01-03T00:00:00Z" ),
                                                           Duration.ofHours( 2 ),
                                                           Duration.ofHours( 5 ) );
        TimeWindow four = MessageUtilities.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
                                                          Instant.parse( "1934-01-02T00:00:00Z" ),
                                                          Instant.parse( "1935-01-01T00:00:00Z" ),
                                                          Instant.parse( "1935-01-02T00:00:00Z" ),
                                                          Duration.ofHours( 3 ),
                                                          Duration.ofHours( 4 ) );

        Set<TimeWindowOuter> first = Set.of( TimeWindowOuter.of( one ),
                                             TimeWindowOuter.of( two ) );
        Set<TimeWindowOuter> second = Set.of( TimeWindowOuter.of( three ),
                                              TimeWindowOuter.of( four ) );

        Set<TimeWindowOuter> actual = TimeWindowSlicer.intersection( first, second );
        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( one ),
                                                TimeWindowOuter.of( two ),
                                                TimeWindowOuter.of( three ) );

        assertEquals( expected, actual );
    }

    @Test
    void testIntersects()
    {
        // Lead duration only
        TimeWindow one = MessageUtilities.getTimeWindow( Duration.ofHours( 1 ),
                                                         Duration.ofHours( 3 ) );
        TimeWindow two = MessageUtilities.getTimeWindow( Duration.ofHours( 3 ),
                                                         Duration.ofHours( 5 ) );

        // Valid time only
        TimeWindow three = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                           Instant.parse( INSTANT_TWO ) );
        TimeWindow four = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                          Instant.parse( INSTANT_THREE ) );

        // Reference time only
        TimeWindow five = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                          Instant.parse( INSTANT_TWO ),
                                                          Instant.MIN,
                                                          Instant.MAX );
        TimeWindow six = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                         Instant.parse( INSTANT_THREE ),
                                                         Instant.MIN,
                                                         Instant.MAX );

        // All dimensions
        TimeWindow seven = MessageUtilities.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
                                                           Instant.parse( "1934-01-02T00:00:00Z" ),
                                                           Instant.parse( "1935-01-01T00:00:00Z" ),
                                                           Instant.parse( "1935-01-02T00:00:00Z" ),
                                                           Duration.ofHours( 1 ),
                                                           Duration.ofHours( 2 ) );
        TimeWindow eight = MessageUtilities.getTimeWindow( Instant.parse( "1933-01-01T00:00:00Z" ),
                                                           Instant.parse( "1934-01-02T00:00:00Z" ),
                                                           Instant.parse( "1934-01-01T00:00:00Z" ),
                                                           Instant.parse( "1935-01-02T00:00:00Z" ),
                                                           Duration.ofHours( 1 ),
                                                           Duration.ofHours( 2 ) );

        // No intersection
        TimeWindow nine = MessageUtilities.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
                                                          Instant.parse( "1934-01-02T00:00:00Z" ),
                                                          Instant.parse( "1935-01-01T00:00:00Z" ),
                                                          Instant.parse( "1935-01-02T00:00:00Z" ),
                                                          Duration.ofHours( 1 ),
                                                          Duration.ofHours( 2 ) );
        TimeWindow ten = MessageUtilities.getTimeWindow( Instant.parse( "2033-01-01T00:00:00Z" ),
                                                         Instant.parse( "2034-01-02T00:00:00Z" ),
                                                         Instant.parse( "2034-01-01T00:00:00Z" ),
                                                         Instant.parse( "2035-01-02T00:00:00Z" ),
                                                         Duration.ofHours( 3 ),
                                                         Duration.ofHours( 4 ) );

        assertAll( () -> assertTrue( TimeWindowSlicer.intersects( TimeWindowOuter.of( one ),
                                                                  TimeWindowOuter.of( two ) ) ),
                   () -> assertTrue( TimeWindowSlicer.intersects( TimeWindowOuter.of( three ),
                                                                  TimeWindowOuter.of( four ) ) ),
                   () -> assertTrue( TimeWindowSlicer.intersects( TimeWindowOuter.of( five ),
                                                                  TimeWindowOuter.of( six ) ) ),
                   () -> assertTrue( TimeWindowSlicer.intersects( TimeWindowOuter.of( seven ),
                                                                  TimeWindowOuter.of( eight ) ) ),
                   () -> assertFalse( TimeWindowSlicer.intersects( TimeWindowOuter.of( nine ),
                                                                   TimeWindowOuter.of( ten ) ) ) );
    }

    @Test
    void testAdjustByTimeScalePeriodWhenTimeScaleIsInstantaneous()
    {
        TimeWindow timeWindow = MessageUtilities.getTimeWindow( Duration.ofHours( 1 ),
                                                                Duration.ofHours( 2 ) );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                               .setSeconds( 1 ) )
                                       .build();
        TimeScaleOuter timeScaleOuter = TimeScaleOuter.of( timeScale );
        TimeWindowOuter timeWindowOuter = TimeWindowOuter.of( timeWindow );

        TimeWindowOuter actual = TimeWindowSlicer.adjustTimeWindowForTimeScale( timeWindowOuter, timeScaleOuter );

        assertEquals( timeWindowOuter, actual );
    }

    @Test
    void testAdjustTimeWindowEarliestLeadDurationForTimeScale()
    {
        TimeWindow timeWindow = MessageUtilities.getTimeWindow( Duration.ofHours( 1 ),
                                                                Duration.ofHours( 2 ) );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                               .setSeconds( 1800 ) )
                                       .build();
        TimeScaleOuter timeScaleOuter = TimeScaleOuter.of( timeScale );
        TimeWindowOuter timeWindowOuter = TimeWindowOuter.of( timeWindow );

        TimeWindowOuter actual = TimeWindowSlicer.adjustTimeWindowForTimeScale( timeWindowOuter, timeScaleOuter );
        TimeWindow expectedInner = MessageUtilities.getTimeWindow( Duration.ofMinutes( 30 ),
                                                                   Duration.ofHours( 2 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );

        assertEquals( expected, actual );
    }

    @Test
    void testAdjustTimeWindowEarliestValidTimeForTimeScale()
    {
        Instant earliest = Instant.parse( "2055-03-23T00:00:00Z" );
        Instant latest = Instant.parse( "2055-03-24T00:00:00Z" );

        TimeWindow timeWindow = MessageUtilities.getTimeWindow( earliest, latest );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                               .setSeconds( 86400 ) )
                                       .build();
        TimeScaleOuter timeScaleOuter = TimeScaleOuter.of( timeScale );
        TimeWindowOuter timeWindowOuter = TimeWindowOuter.of( timeWindow );

        TimeWindowOuter actual = TimeWindowSlicer.adjustTimeWindowForTimeScale( timeWindowOuter, timeScaleOuter );
        TimeWindow expectedInner = MessageUtilities.getTimeWindow( Instant.parse( "2055-03-22T00:00:00Z" ),
                                                                   latest );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );

        assertEquals( expected, actual );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code> and a <code>lead_time_pools</code>. Expects twenty-four time windows
     * with prescribed characteristics.
     *
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated
     * with system test scenario017, as of commit fa548da9da85b16631f238f78b358d85ddbebed5.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsReturnsTwentyFourWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 24 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 1 ) )
                                                                              .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 24 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 1 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 1 ),
                                                                 Duration.ofHours( 2 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 2 ),
                                                                 Duration.ofHours( 3 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 3 ),
                                                                 Duration.ofHours( 4 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 4 ),
                                                                 Duration.ofHours( 5 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 5 ),
                                                                 Duration.ofHours( 6 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 6 ),
                                                                 Duration.ofHours( 7 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 7 ),
                                                                 Duration.ofHours( 8 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 8 ),
                                                                 Duration.ofHours( 9 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 9 ),
                                                                 Duration.ofHours( 10 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 10 ),
                                                                 Duration.ofHours( 11 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 11 ),
                                                                 Duration.ofHours( 12 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 12 ),
                                                                 Duration.ofHours( 13 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 13 ),
                                                                 Duration.ofHours( 14 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 14 ),
                                                                 Duration.ofHours( 15 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 15 ),
                                                                 Duration.ofHours( 16 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 16 ),
                                                                 Duration.ofHours( 17 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 18 ),
                                                                 Duration.ofHours( 19 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 19 ),
                                                                 Duration.ofHours( 20 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 20 ),
                                                                 Duration.ofHours( 21 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 21 ),
                                                                 Duration.ofHours( 22 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 22 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 23 ),
                                                                 Duration.ofHours( 24 ) ) );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 24, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code> and a <code>lead_time_pools</code>. Expects two time windows with
     * prescribed characteristics.
     *
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated
     * with system test scenario403, as of commit fa548da9da85b16631f238f78b358d85ddbebed5.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsReturnsTwoWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 48 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 24 ) )
                                                                              .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 2 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 24 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 24 ),
                                                                 Duration.ofHours( 48 ) ) );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 2, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code>, a <code>lead_time_pools</code>, an <code>reference_dates</code>, and an
     * <code>referenceDatesPoolingWindow</code>. Expects eighteen time windows with prescribed characteristics.
     *
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated
     * with system test scenario505, which is in development as of commit 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesReferenceDatesLeadTimePoolsAndReferenceDatesPoolingWindowReturnsEighteenWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 40 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 23 ) )
                                                                              .frequency( Duration.ofHours( 17 ) )
                                                                              .build() );

        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_NINE ) )
                                                         .maximum( Instant.parse( INSTANT_TEN ) )
                                                         .build();
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 13 ) )
                                                                                   .frequency( Duration.ofHours( 7 ) )
                                                                                   .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 18 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                                 //2551-03-17T00:00:00Z
                                                                 Instant.parse( INSTANT_TWELVE ),
                                                                 //2551-03-17T13:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                                 //2551-03-17T00:00:00Z
                                                                 Instant.parse( INSTANT_TWELVE ),
                                                                 //2551-03-17T13:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                                 //2551-03-17T07:00:00Z
                                                                 Instant.parse( INSTANT_FOURTEEN ),
                                                                 //2551-03-17T20:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                                 //2551-03-17T07:00:00Z
                                                                 Instant.parse( INSTANT_FOURTEEN ),
                                                                 //2551-03-17T20:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                                 //2551-03-17T14:00:00Z
                                                                 Instant.parse( INSTANT_SIXTEEN ),
                                                                 //2551-03-18T03:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                                 //2551-03-17T14:00:00Z
                                                                 Instant.parse( INSTANT_SIXTEEN ),
                                                                 //2551-03-18T03:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                                 //2551-03-17T21:00:00Z
                                                                 Instant.parse( INSTANT_EIGHTEEN ),
                                                                 //2551-03-18T10:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                                 //2551-03-17T21:00:00Z
                                                                 Instant.parse( INSTANT_EIGHTEEN ),
                                                                 //2551-03-18T10:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                                 //2551-03-18T04:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY ),
                                                                 //2551-03-18T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                                 //2551-03-18T04:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY ),
                                                                 //2551-03-18T17:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                                 //2551-03-18T11:00:00Z
                                                                 Instant.parse( INSTANT_ELEVEN ),
                                                                 //2551-03-19T00:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                                 //2551-03-18T11:00:00Z
                                                                 Instant.parse( INSTANT_ELEVEN ),
                                                                 //2551-03-19T00:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                                 //2551-03-18T18:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_THREE ),
                                                                 //2551-03-19T07:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                                 //2551-03-18T18:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_THREE ),
                                                                 //2551-03-19T07:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                                 //2551-03-19T01:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_FIVE ),
                                                                 //2551-03-19T14:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                                 //2551-03-19T01:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_FIVE ),
                                                                 //2551-03-19T14:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
                                                                 //2551-03-19T08:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_SEVEN ),
                                                                 //2551-03-19T21:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
                                                                 //2551-03-19T08:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_SEVEN ),
                                                                 //2551-03-19T21:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) ) );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 18, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code>, a <code>lead_time_pools</code>, a <code>dates</code> and an
     * <code>reference_dates</code>. Expects one time window with prescribed characteristics.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesValidDatesReferenceDatesAndLeadTimePoolsReturnsOneWindow()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 1 ) )
                                                            .maximum( Duration.ofHours( 48 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 24 ) )
                                                                              .build() );
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_NINE ) )
                                                         .maximum( Instant.parse( INSTANT_TEN ) )
                                                         .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ELEVEN ) )
                                                     .maximum( Instant.parse( INSTANT_TWENTY_EIGHT ) )
                                                     .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .validDates( validDates )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                                 //2551-03-17T00:00:00Z
                                                                 Instant.parse( INSTANT_TEN ),
                                                                 //2551-03-20T00:00:00Z
                                                                 Instant.parse( INSTANT_ELEVEN ),
                                                                 //2551-03-19T00:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_EIGHT ),
                                                                 //2551-03-24T00:00:00Z
                                                                 Duration.ofHours( 1 ),
                                                                 Duration.ofHours( 25 ) ) );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * does not include any constraints on time, aka "one big pool".
     *
     * <p>This is analogous to system test scenario508, as of commit b9a7214ec22999482784119a8527149348c80119.
     */

    @Test
    void testGetTimeWindowsFromUnconstrainedDeclarationReturnsOneWindow()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.MIN,
                                                                 Instant.MAX,
                                                                 MessageUtilities.DURATION_MIN,
                                                                 MessageUtilities.DURATION_MAX ) );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>referenceDatesPoolingWindow</code> and a <code>lead_time_pools</code>. Expects twenty-three
     * time windows. Tests both an explicit and implicit declaration of the <code>frequency</code>.
     *
     * <p>The project declaration from this test matches the declaration associated
     * with system test scenario704, as of commit da07c16148429740496b8cc6df89a73e3697f17c,
     * except the <code>period</code> is 1.0 time units here.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesValidDatesReferenceDatesReferenceDatesPoolingWindowAndLeadTimePoolsReturnsTwentyThreeWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 18 ) )
                                                                              .build() );
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_ONE ) )
                                                         .maximum( Instant.parse( INSTANT_TWO ) )
                                                         .build();
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 1 ) )
                                                                                   .frequency( Duration.ofHours( 1 ) )
                                                                                   .build() );
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_THREE ) )
                                                     .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .validDates( validDates )
                                                                        .build();

        // Generate the expected time windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 23 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_FOUR ),
                                                                 //2017-08-08T01:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                                 //2017-08-08T01:00:00Z
                                                                 Instant.parse( INSTANT_FIVE ),
                                                                 //2017-08-08T02:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FIVE ),
                                                                 //2017-08-08T02:00:00Z
                                                                 Instant.parse( INSTANT_SIX ),
                                                                 //2017-08-08T03:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_SIX ),
                                                                 //2017-08-08T03:00:00Z
                                                                 Instant.parse( INSTANT_SEVEN ),
                                                                 //2017-08-08T04:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_SEVEN ),
                                                                 //2017-08-08T04:00:00Z
                                                                 Instant.parse( INSTANT_EIGHT ),
                                                                 //2017-08-08T05:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_EIGHT ),
                                                                 //2017-08-08T05:00:00Z
                                                                 Instant.parse( "2017-08-08T06:00:00Z" ),
                                                                 //2017-08-08T06:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T06:00:00Z" ),
                                                                 //2017-08-08T06:00:00Z
                                                                 Instant.parse( "2017-08-08T07:00:00Z" ),
                                                                 //2017-08-08T07:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T07:00:00Z" ),
                                                                 //2017-08-08T07:00:00Z
                                                                 Instant.parse( "2017-08-08T08:00:00Z" ),
                                                                 //2017-08-08T08:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T08:00:00Z" ),
                                                                 //2017-08-08T08:00:00Z
                                                                 Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                 //2017-08-08T09:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                 //2017-08-08T09:00:00Z
                                                                 Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                 //2017-08-08T10:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                 //2017-08-08T10:00:00Z
                                                                 Instant.parse( "2017-08-08T11:00:00Z" ),
                                                                 //2017-08-08T11:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T11:00:00Z" ),
                                                                 //2017-08-08T11:00:00Z
                                                                 Instant.parse( "2017-08-08T12:00:00Z" ),
                                                                 //2017-08-08T12:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T12:00:00Z" ),
                                                                 //2017-08-08T12:00:00Z
                                                                 Instant.parse( "2017-08-08T13:00:00Z" ),
                                                                 //2017-08-08T13:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T13:00:00Z" ),
                                                                 //2017-08-08T13:00:00Z
                                                                 Instant.parse( "2017-08-08T14:00:00Z" ),
                                                                 //2017-08-08T14:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T14:00:00Z" ),
                                                                 //2017-08-08T14:00:00Z
                                                                 Instant.parse( "2017-08-08T15:00:00Z" ),
                                                                 //2017-08-08T15:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T15:00:00Z" ),
                                                                 //2017-08-08T15:00:00Z
                                                                 Instant.parse( "2017-08-08T16:00:00Z" ),
                                                                 //2017-08-08T16:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T16:00:00Z" ),
                                                                 //2017-08-08T16:00:00Z
                                                                 Instant.parse( "2017-08-08T17:00:00Z" ),
                                                                 //2017-08-08T17:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T17:00:00Z" ),
                                                                 //2017-08-08T17:00:00Z
                                                                 Instant.parse( "2017-08-08T18:00:00Z" ),
                                                                 //2017-08-08T18:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T18:00:00Z" ),
                                                                 //2017-08-08T18:00:00Z
                                                                 Instant.parse( "2017-08-08T19:00:00Z" ),
                                                                 //2017-08-08T19:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T19:00:00Z" ),
                                                                 //2017-08-08T19:00:00Z
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 //2017-08-08T20:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 //2017-08-08T20:00:00Z
                                                                 Instant.parse( "2017-08-08T21:00:00Z" ),
                                                                 //2017-08-08T21:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T21:00:00Z" ),
                                                                 //2017-08-08T21:00:00Z
                                                                 Instant.parse( "2017-08-08T22:00:00Z" ),
                                                                 //2017-08-08T22:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T22:00:00Z" ),
                                                                 //2017-08-08T22:00:00Z
                                                                 Instant.parse( INSTANT_TWO ),
                                                                 //2017-08-08T23:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );

        // Generate the actual time windows for the explicit test
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 23, actualTimeWindows.size() );

        // Assert that the expected and actual time windows are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );

        // Declare the same version of this test with implicit frequency
        Set<TimePools> referenceTimePoolsNoFreq = Collections.singleton( TimePoolsBuilder.builder()
                                                                                         .period( Duration.ofHours( 1 ) )
                                                                                         .build() );
        EvaluationDeclaration declarationNoFreq =
                EvaluationDeclarationBuilder.builder()
                                            .leadTimes( leadTimes )
                                            .leadTimePools( leadTimePools )
                                            .referenceDates( referenceDates )
                                            .referenceDatePools( referenceTimePoolsNoFreq )
                                            .validDates( validDates )
                                            .build();

        // Generate the actual time windows for the implicit test
        Set<TimeWindowOuter> actualTimeWindowsNoFreq = TimeWindowSlicer.getTimeWindows( declarationNoFreq );

        // Assert that the expected and actual time windows are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindowsNoFreq );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * does not include any explicit time windows, but is constrained by <code>lead_times</code>,
     * <code>reference_dates</code> and <code>dates</code>, aka "one big pool" with constraints.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesValidDatesAndReferenceDatesReturnsOneWindow()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_ONE ) )
                                                         .maximum( Instant.parse( INSTANT_TWO ) )
                                                         .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_THREE ) )
                                                     .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .referenceDates( referenceDates )
                                                                        .validDates( validDates )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_TWO ),
                                                                 //2017-08-08T23:00:00Z
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 //2017-08-08T00:00:00Z
                                                                 Instant.parse( INSTANT_THREE ),
                                                                 //2017-08-09T17:00:00Z
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 18 ) ) );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code>, a an <code>reference_dates</code>, and an <code>referenceDatesPoolingWindow</code>.
     * Expects nine time windows with prescribed characteristics.
     *
     * <p>The project declaration from this test scenario is similar to the declaration associated
     * with system test scenario505, as of commit c8def0cf2d608c0617786f7cb4f28b563960d667, but without
     * a <code>lead_time_pools</code>.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesReferenceDatesAndReferenceDatesPoolingWindowAndNoLeadTimePoolsReturnsNineWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 40 ) )
                                                            .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_NINE ) )
                                                         .maximum( Instant.parse( INSTANT_TEN ) )
                                                         .build();
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 13 ) )
                                                                                   .frequency( Duration.ofHours( 7 ) )
                                                                                   .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 9 );

        Duration first = Duration.ofHours( 0 );
        Duration last = Duration.ofHours( 40 );

        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                                 //2551-03-17T00:00:00Z
                                                                 Instant.parse( INSTANT_TWELVE ),
                                                                 //2551-03-17T13:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                                 //2551-03-17T07:00:00Z
                                                                 Instant.parse( INSTANT_FOURTEEN ),
                                                                 //2551-03-17T20:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                                 //2551-03-17T14:00:00Z
                                                                 Instant.parse( INSTANT_SIXTEEN ),
                                                                 //2551-03-18T03:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                                 //2551-03-17T21:00:00Z
                                                                 Instant.parse( INSTANT_EIGHTEEN ),
                                                                 //2551-03-18T10:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                                 //2551-03-18T04:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY ),
                                                                 //2551-03-18T17:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                                 //2551-03-18T11:00:00Z
                                                                 Instant.parse( INSTANT_ELEVEN ),
                                                                 //2551-03-19T00:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                                 //2551-03-18T18:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_THREE ),
                                                                 //2551-03-19T07:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                                 //2551-03-19T01:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_FIVE ),
                                                                 //2551-03-19T14:00:00Z
                                                                 first,
                                                                 last ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
                                                                 //2551-03-19T08:00:00Z
                                                                 Instant.parse( INSTANT_TWENTY_SEVEN ),
                                                                 //2551-03-19T21:00:00Z
                                                                 first,
                                                                 last ) );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 9, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code> and an <code>lead_time_pools</code> and the <code>minimum</code> and
     * <code>maximum</code> lead hours are the same value and the period associated with the
     * <code>lead_time_pools</code> is zero wide. This is equivalent to system test scenario010 as of commit
     * 8480aa4d4ddc09275746fe590623ecfd83e452ae and is used to check that a zero-wide pool centered on a single lead
     * duration does not increment infinitely.
     */

    @Test
    void testGetTimeWindowsWithZeroWideLeadTimesAndLeadTimePoolsWithZeroPeriodReturnsOneWindow()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 43 ) )
                                                            .maximum( Duration.ofHours( 43 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ZERO )
                                                                              .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );

        Duration first = Duration.ofHours( 43 );
        Duration last = Duration.ofHours( 43 );

        TimeWindow inner = MessageUtilities.getTimeWindow( first,
                                                           last );
        expectedTimeWindows.add( inner );

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    @Test
    void testGetTimeWindowsWithValidDatesAndValidDatePoolsReturnsTwoWindows()
    {
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        Set<TimePools> validTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                               .period( Duration.ofHours( 13 ) )
                                                                               .frequency( Duration.ofHours( 7 ) )
                                                                               .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 2, actualTimeWindows.size() );

        // Generate the expected windows
        Set<TimeWindowOuter> expectedTimeWindows = new HashSet<>( 2 );

        TimeWindow innerOne = MessageUtilities.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                              Instant.parse( "2017-08-08T13:00:00Z" ) );
        TimeWindow innerTwo = MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T07:00:00Z" ),
                                                              Instant.parse( "2017-08-08T20:00:00Z" ) );

        expectedTimeWindows.add( TimeWindowOuter.of( innerOne ) );
        expectedTimeWindows.add( TimeWindowOuter.of( innerTwo ) );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsAndValidDatesAndValidDatePoolsReturnsFourWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 19 ) )
                                                            .maximum( Duration.ofHours( 34 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 8 ) )
                                                                              .frequency( Duration.ofHours( 7 ) )
                                                                              .build() );
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        Set<TimePools> validTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                               .period( Duration.ofHours( 13 ) )
                                                                               .frequency( Duration.ofHours( 7 ) )
                                                                               .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 4, actualTimeWindows.size() );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 4 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 Instant.parse( "2017-08-08T13:00:00Z" ),
                                                                 Duration.ofHours( 19 ),
                                                                 Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( "2017-08-08T07:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 Duration.ofHours( 19 ),
                                                                 Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 Instant.parse( "2017-08-08T13:00:00Z" ),
                                                                 Duration.ofHours( 26 ),
                                                                 Duration.ofHours( 34 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( "2017-08-08T07:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 Duration.ofHours( 26 ),
                                                                 Duration.ofHours( 34 ) ) );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsAndValidDatesAndValidDatePoolsAndReferenceDatesAndReferenceDatePoolsReturnsFourWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 19 ) )
                                                            .maximum( Duration.ofHours( 34 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 8 ) )
                                                                              .frequency( Duration.ofHours( 7 ) )
                                                                              .build() );
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_TWO ) )
                                                         .maximum( Instant.parse( INSTANT_THREE ) )
                                                         .build();
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 17 ) )
                                                                                   .frequency( Duration.ofHours( 23 ) )
                                                                                   .build() );
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        Set<TimePools> validTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                               .period( Duration.ofHours( 13 ) )
                                                                               .frequency( Duration.ofHours( 7 ) )
                                                                               .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindowOuter> actualTimeWindows = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 4, actualTimeWindows.size() );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 4 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                                 Instant.parse( "2017-08-09T16:00:00Z" ),
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 Instant.parse( "2017-08-08T13:00:00Z" ),
                                                                 Duration.ofHours( 19 ),
                                                                 Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                                 Instant.parse( "2017-08-09T16:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T07:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 Duration.ofHours( 19 ),
                                                                 Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                                 Instant.parse( "2017-08-09T16:00:00Z" ),
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 Instant.parse( "2017-08-08T13:00:00Z" ),
                                                                 Duration.ofHours( 26 ),
                                                                 Duration.ofHours( 34 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                                 Instant.parse( "2017-08-09T16:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T07:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 Duration.ofHours( 26 ),
                                                                 Duration.ofHours( 34 ) ) );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows.stream()
                                         .map( TimeWindowOuter::of )
                                         .collect( Collectors.toSet() ),
                      actualTimeWindows );
    }

    @Test
    void testGetTimeWindowsWithTwoSequencesOfLeadTimePoolsReturnsFourWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 1 ) )
                                                            .maximum( Duration.ofHours( 5 ) )
                                                            .build();
        TimePools leadTimePoolsOne = TimePoolsBuilder.builder()
                                                     .period( Duration.ofHours( 1 ) )
                                                     .frequency( Duration.ofHours( 3 ) )
                                                     .build();
        TimePools leadTimePoolsTwo = TimePoolsBuilder.builder()
                                                     .period( Duration.ofHours( 1 ) )
                                                     .frequency( Duration.ofHours( 2 ) )
                                                     .build();

        Set<TimePools> leadTimePools = Set.of( leadTimePoolsOne, leadTimePoolsTwo );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindowOuter> actual = TimeWindowSlicer.getTimeWindows( declaration );

        TimeWindow oneExpected = MessageUtilities.getTimeWindow( Duration.ofHours( 1 ), Duration.ofHours( 2 ) );
        TimeWindow twoExpected = MessageUtilities.getTimeWindow( Duration.ofHours( 3 ), Duration.ofHours( 4 ) );
        TimeWindow threeExpected = MessageUtilities.getTimeWindow( Duration.ofHours( 4 ), Duration.ofHours( 5 ) );

        Set<TimeWindow> expectedInner = Set.of( oneExpected, twoExpected, threeExpected );
        Set<TimeWindowOuter> expected = expectedInner.stream()
                                                     .map( TimeWindowOuter::of )
                                                     .collect( Collectors.toSet() );
        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeWindowsWithTwoSequencesOfLeadTimePoolsAndValidDatePoolsReturnsTwelveWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 4 ) )
                                                            .build();
        TimePools leadTimePoolOne = TimePoolsBuilder.builder()
                                                    .period( Duration.ofHours( 2 ) )
                                                    .build();
        TimePools leadTimePoolTwo = TimePoolsBuilder.builder()
                                                    .period( Duration.ofHours( 3 ) )
                                                    .build();

        Set<TimePools> leadTimePools = Set.of( leadTimePoolOne, leadTimePoolTwo );
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        TimePools validDatePoolOne = TimePoolsBuilder.builder()
                                                     .period( Duration.ofHours( 10 ) )
                                                     .build();
        TimePools validDatePoolTwo = TimePoolsBuilder.builder()
                                                     .period( Duration.ofHours( 9 ) )
                                                     .build();

        Set<TimePools> validTimePools = Set.of( validDatePoolOne, validDatePoolTwo );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindowOuter> actual = TimeWindowSlicer.getTimeWindows( declaration );

        TimeWindow expectedOne = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( INSTANT_ONE ),
                                                                 Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 2 ) );
        TimeWindow expectedTwo = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 2 ) );
        TimeWindow expectedThree = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                   Instant.MAX,
                                                                   Instant.parse( INSTANT_ONE ),
                                                                   Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                   Duration.ofHours( 0 ),
                                                                   Duration.ofHours( 2 ) );
        TimeWindow expectedFour = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                  Instant.MAX,
                                                                  Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                  Instant.parse( "2017-08-08T18:00:00Z" ),
                                                                  Duration.ofHours( 0 ),
                                                                  Duration.ofHours( 2 ) );
        TimeWindow expectedFive = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                  Instant.MAX,
                                                                  Instant.parse( INSTANT_ONE ),
                                                                  Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                  Duration.ofHours( 2 ),
                                                                  Duration.ofHours( 4 ) );
        TimeWindow expectedSix = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 Duration.ofHours( 2 ),
                                                                 Duration.ofHours( 4 ) );
        TimeWindow expectedSeven = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                   Instant.MAX,
                                                                   Instant.parse( INSTANT_ONE ),
                                                                   Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                   Duration.ofHours( 2 ),
                                                                   Duration.ofHours( 4 ) );
        TimeWindow expectedEight = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                   Instant.MAX,
                                                                   Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                   Instant.parse( "2017-08-08T18:00:00Z" ),
                                                                   Duration.ofHours( 2 ),
                                                                   Duration.ofHours( 4 ) );
        TimeWindow expectedNine = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                  Instant.MAX,
                                                                  Instant.parse( INSTANT_ONE ),
                                                                  Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                  Duration.ofHours( 0 ),
                                                                  Duration.ofHours( 3 ) );
        TimeWindow expectedTen = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                 Instant.MAX,
                                                                 Instant.parse( "2017-08-08T10:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T20:00:00Z" ),
                                                                 Duration.ofHours( 0 ),
                                                                 Duration.ofHours( 3 ) );
        TimeWindow expectedEleven = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                    Instant.MAX,
                                                                    Instant.parse( INSTANT_ONE ),
                                                                    Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                    Duration.ofHours( 0 ),
                                                                    Duration.ofHours( 3 ) );
        TimeWindow expectedTwelve = MessageUtilities.getTimeWindow( Instant.MIN,
                                                                    Instant.MAX,
                                                                    Instant.parse( "2017-08-08T09:00:00Z" ),
                                                                    Instant.parse( "2017-08-08T18:00:00Z" ),
                                                                    Duration.ofHours( 0 ),
                                                                    Duration.ofHours( 3 ) );

        Set<TimeWindow> expectedInner = Set.of( expectedOne,
                                                expectedTwo,
                                                expectedThree,
                                                expectedFour,
                                                expectedFive,
                                                expectedSix,
                                                expectedSeven,
                                                expectedEight,
                                                expectedNine,
                                                expectedTen,
                                                expectedEleven,
                                                expectedTwelve );

        Set<TimeWindowOuter> expected = expectedInner.stream()
                                                     .map( TimeWindowOuter::of )
                                                     .collect( Collectors.toSet() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeWindowsWithValidDatesAndValidDatePoolsInReverseReturnsTwoWindows()
    {
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        TimePools validDatePoolOne = TimePoolsBuilder.builder()
                                                     .period( Duration.ofHours( 10 ) )
                                                     .reverse( true )
                                                     .build();

        Set<TimePools> validTimePools = Set.of( validDatePoolOne );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindowOuter> actual = TimeWindowSlicer.getTimeWindows( declaration );

        TimeWindow expectedOne = MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T13:00:00Z" ),
                                                                 Instant.parse( INSTANT_TWO ) );

        TimeWindow expectedTwo = MessageUtilities.getTimeWindow( Instant.parse( "2017-08-08T03:00:00Z" ),
                                                                 Instant.parse( "2017-08-08T13:00:00Z" ) );

        Set<TimeWindow> expectedInner = Set.of( expectedOne, expectedTwo );

        Set<TimeWindowOuter> expected = expectedInner.stream()
                                                     .map( TimeWindowOuter::of )
                                                     .collect( Collectors.toSet() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsInReverseReturnsThreeWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 24 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 7 ) )
                                                                              .reverse( true )
                                                                              .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 3 );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 3 ),
                                                                 Duration.ofHours( 10 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 10 ),
                                                                 Duration.ofHours( 17 ) ) );
        expectedTimeWindows.add( MessageUtilities.getTimeWindow( Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 24 ) ) );

        Set<TimeWindowOuter> expected = expectedTimeWindows.stream()
                                                           .map( TimeWindowOuter::of )
                                                           .collect( Collectors.toSet() );

        // Generate the actual windows
        Set<TimeWindowOuter> actual = TimeWindowSlicer.getTimeWindows( declaration );

        // Assert that the expected and actual are equal
        assertEquals( expected, actual );
    }

    @Test
    void testAggregateTimeWindowsWithMaximum()
    {
        Set<TimeWindowOuter> timeWindows = this.getTimeWindowsForAggregationTesting();
        TimeWindowOuter actual = TimeWindowSlicer.aggregate( timeWindows, TimeWindowAggregation.MAXIMUM );
        TimeWindow expectedInner = MessageUtilities.getTimeWindow( Instant.parse( "2088-08-08T23:00:00Z" ),
                                                                   Instant.parse( "2110-08-09T16:00:00Z" ),
                                                                   Instant.parse( "2080-08-08T07:00:00Z" ),
                                                                   Instant.parse( "2102-08-08T20:00:00Z" ),
                                                                   Duration.ofHours( 13 ),
                                                                   Duration.ofHours( 39 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );

        assertEquals( expected, actual );
    }

    @Test
    void testAggregateTimeWindowsWithMinimum()
    {
        Set<TimeWindowOuter> timeWindows = this.getTimeWindowsForAggregationTesting();
        TimeWindowOuter actual = TimeWindowSlicer.aggregate( timeWindows, TimeWindowAggregation.MINIMUM );
        TimeWindow expectedInner = MessageUtilities.getTimeWindow( Instant.parse( "2099-08-08T23:00:00Z" ),
                                                                   Instant.parse( "2106-08-09T16:00:00Z" ),
                                                                   Instant.parse( "2099-08-08T07:00:00Z" ),
                                                                   Instant.parse( "2100-08-08T13:00:00Z" ),
                                                                   Duration.ofHours( 20 ),
                                                                   Duration.ofHours( 27 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );

        assertEquals( expected, actual );
    }

    @Test
    void testAggregateTimeWindowsWithAverage()
    {
        Set<TimeWindowOuter> timeWindows = this.getTimeWindowsForAggregationTesting();
        TimeWindowOuter actual = TimeWindowSlicer.aggregate( timeWindows, TimeWindowAggregation.AVERAGE );
        TimeWindow expectedInner = MessageUtilities.getTimeWindow( Instant.parse( "2092-08-08T15:00:00Z" ),
                                                                   Instant.parse( "2108-12-09T00:00:00Z" ),
                                                                   Instant.parse( "2090-04-08T09:00:00Z" ),
                                                                   Instant.parse( "2101-08-08T17:40:00Z" ),
                                                                   Duration.ofMinutes( 17 * 60 + 20 ),
                                                                   Duration.ofMinutes( 31 * 60 + 40 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );

        assertEquals( expected, actual );
    }

    /**
     * Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} for an expected exception when
     * <code>lead_times</code> are required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenLeadTimesExpectedButMissing()
    {
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 18 ) )
                                                                              .build() );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine lead duration time windows without 'lead_times'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>minimum</code> <code>lead_times</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenLeadTimesMinimumExpectedButMissing()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .maximum( Duration.ofHours( 40 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 18 ) )
                                                                              .build() );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine lead duration time windows without a 'minimum' value for 'lead_times'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>maximum</code> <code>lead_times</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenLeadTimesMaximumExpectedButMissing()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .build();
        Set<TimePools> leadTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                              .period( Duration.ofHours( 18 ) )
                                                                              .build() );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine lead duration time windows without a 'maximum' value for 'lead_times'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} for an expected exception when
     * <code>reference_dates</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenReferenceDatesExpectedButMissing()
    {
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 13 ) )
                                                                                   .frequency( Duration.ofHours( 7 ) )
                                                                                   .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine reference time windows without 'reference_dates'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>minimum</code> <code>reference_dates</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenReferenceDatesEarliestExpectedButMissing()
    {
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .maximum( Instant.parse( INSTANT_ONE ) )
                                                         .build();
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 13 ) )
                                                                                   .frequency( Duration.ofHours( 7 ) )
                                                                                   .build() );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine reference time windows without the 'minimum' for the 'reference_dates'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowSlicer#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>maximum</code> <code>reference_dates</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenReferenceDatesLatestExpectedButMissing()
    {
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_ONE ) )
                                                         .build();
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 13 ) )
                                                                                   .frequency( Duration.ofHours( 7 ) )
                                                                                   .build() );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine reference time windows without the 'maximum' for the 'reference_dates'.",
                      thrown.getMessage() );
    }

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenValidDatesExpectedButMissing()
    {
        Set<TimePools> validTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                               .period( Duration.ofHours( 13 ) )
                                                                               .frequency( Duration.ofHours( 7 ) )
                                                                               .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine valid time windows without 'valid_dates'.",
                      thrown.getMessage() );
    }

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenValidDatesEarliestExpectedButMissing()
    {
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .maximum( Instant.parse( INSTANT_ONE ) )
                                                     .build();
        Set<TimePools> validTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                               .period( Duration.ofHours( 13 ) )
                                                                               .frequency( Duration.ofHours( 7 ) )
                                                                               .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine valid time windows without the 'minimum' for the 'valid_dates'.",
                      thrown.getMessage() );
    }

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenValidDatesLatestExpectedButMissing()
    {
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .build();
        Set<TimePools> validTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                               .period( Duration.ofHours( 13 ) )
                                                                               .frequency( Duration.ofHours( 7 ) )
                                                                               .build() );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowSlicer.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine valid time windows without the 'maximum' for the 'valid_dates'.",
                      thrown.getMessage() );
    }

    /**
     * @return time windows for aggregation testing
     */

    private Set<TimeWindowOuter> getTimeWindowsForAggregationTesting()
    {
        Set<TimeWindow> timeWindows = new HashSet<>( 3 );
        timeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2099-08-08T23:00:00Z" ),
                                                         Instant.parse( "2109-08-09T16:00:00Z" ),
                                                         Instant.parse( "2090-08-07T13:00:00Z" ),
                                                         Instant.parse( "2100-08-08T13:00:00Z" ),
                                                         Duration.ofHours( 19 ),
                                                         Duration.ofHours( 27 ) ) );
        timeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2088-08-08T23:00:00Z" ),
                                                         Instant.parse( "2110-08-09T16:00:00Z" ),
                                                         Instant.parse( "2080-08-08T07:00:00Z" ),
                                                         Instant.parse( "2101-08-08T20:00:00Z" ),
                                                         Duration.ofHours( 13 ),
                                                         Duration.ofHours( 29 ) ) );
        timeWindows.add( MessageUtilities.getTimeWindow( Instant.parse( "2089-08-08T23:00:00Z" ),
                                                         Instant.parse( "2106-08-09T16:00:00Z" ),
                                                         Instant.parse( "2099-08-08T07:00:00Z" ),
                                                         Instant.parse( "2102-08-08T20:00:00Z" ),
                                                         Duration.ofHours( 20 ),
                                                         Duration.ofHours( 39 ) ) );
        return timeWindows.stream()
                          .map( TimeWindowOuter::of )
                          .collect( Collectors.toUnmodifiableSet() );
    }
}

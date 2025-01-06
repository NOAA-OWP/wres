package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.TimeWindowAggregation;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.MessageFactory;
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
        TimeWindow firstInner = wres.statistics.MessageFactory.getTimeWindow( SECOND_TIME,
                                                                              FOURTH_TIME,
                                                                              Duration.ofHours( 5 ),
                                                                              Duration.ofHours( 25 ) );
        TimeWindowOuter first = TimeWindowOuter.of( firstInner );
        TimeWindow secondInner = wres.statistics.MessageFactory.getTimeWindow( FIRST_TIME,
                                                                               THIRD_TIME,
                                                                               Duration.ofHours( -5 ),
                                                                               Duration.ofHours( 20 ) );
        TimeWindowOuter second = TimeWindowOuter.of( secondInner );
        TimeWindow expectedInner = wres.statistics.MessageFactory.getTimeWindow( FIRST_TIME,
                                                                                 FOURTH_TIME,
                                                                                 Duration.ofHours( -5 ),
                                                                                 Duration.ofHours( 25 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );
        Set<TimeWindowOuter> union = new HashSet<>();
        union.add( first );
        union.add( second );

        TimeWindowOuter actual = TimeWindowSlicer.union( union );

        assertEquals( expected, actual );

        TimeWindow thirdInner = wres.statistics.MessageFactory.getTimeWindow( SECOND_TIME,
                                                                              FOURTH_TIME,
                                                                              FIRST_TIME,
                                                                              Instant.parse( "2019-12-31T11:59:59Z" ),
                                                                              Duration.ofHours( 5 ),
                                                                              Duration.ofHours( 21 ) );
        TimeWindowOuter third = TimeWindowOuter.of( thirdInner );
        TimeWindow fourthInner = wres.statistics.MessageFactory.getTimeWindow( FIRST_TIME,
                                                                               THIRD_TIME,
                                                                               Instant.parse( "1982-01-01T00:00:00Z" ),
                                                                               FOURTH_TIME,
                                                                               Duration.ZERO,
                                                                               Duration.ofHours( 20 ) );
        TimeWindowOuter fourth = TimeWindowOuter.of( fourthInner );
        TimeWindow fifthInner = wres.statistics.MessageFactory.getTimeWindow( FIRST_TIME,
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
        TimeWindow one = MessageFactory.getTimeWindow( Duration.ofHours( 1 ),
                                                       Duration.ofHours( 3 ) );
        TimeWindow two = MessageFactory.getTimeWindow( Duration.ofHours( 3 ),
                                                       Duration.ofHours( 5 ) );
        TimeWindow three = MessageFactory.getTimeWindow( Duration.ofHours( 1 ),
                                                         Duration.ofHours( 2 ) );
        TimeWindow four = MessageFactory.getTimeWindow( Duration.ofHours( 6 ),
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
        TimeWindow one = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                       Instant.parse( INSTANT_TWO ) );
        TimeWindow two = MessageFactory.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                       Instant.parse( INSTANT_THREE ) );
        TimeWindow three = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                         Instant.parse( INSTANT_THREE ) );
        TimeWindow four = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ELEVEN ),
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
    void testIntersectionOnReferenceTimeOnly()
    {
        TimeWindow one = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                       Instant.parse( INSTANT_TWO ),
                                                       Instant.MIN,
                                                       Instant.MAX );
        TimeWindow two = MessageFactory.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                       Instant.parse( INSTANT_THREE ),
                                                       Instant.MIN,
                                                       Instant.MAX );
        TimeWindow three = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                         Instant.parse( INSTANT_THREE ),
                                                         Instant.MIN,
                                                         Instant.MAX );
        TimeWindow four = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ELEVEN ),
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
        TimeWindow one = MessageFactory.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
                                                       Instant.parse( "1934-01-02T00:00:00Z" ),
                                                       Instant.parse( "1935-01-01T00:00:00Z" ),
                                                       Instant.parse( "1935-01-02T00:00:00Z" ),
                                                       Duration.ofHours( 1 ),
                                                       Duration.ofHours( 2 ) );
        TimeWindow two = MessageFactory.getTimeWindow( Instant.parse( "1933-01-01T00:00:00Z" ),
                                                       Instant.parse( "1934-01-02T00:00:00Z" ),
                                                       Instant.parse( "1934-01-01T00:00:00Z" ),
                                                       Instant.parse( "1935-01-02T00:00:00Z" ),
                                                       Duration.ofHours( 1 ),
                                                       Duration.ofHours( 2 ) );
        TimeWindow three = MessageFactory.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
                                                         Instant.parse( "1934-01-04T00:00:00Z" ),
                                                         Instant.parse( "1935-01-01T00:00:00Z" ),
                                                         Instant.parse( "1935-01-03T00:00:00Z" ),
                                                         Duration.ofHours( 2 ),
                                                         Duration.ofHours( 5 ) );
        TimeWindow four = MessageFactory.getTimeWindow( Instant.parse( "1934-01-01T00:00:00Z" ),
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
    void testAdjustByTimeScalePeriodWhenTimeScaleIsInstantaneous()
    {
        TimeWindow timeWindow = MessageFactory.getTimeWindow( Duration.ofHours( 1 ),
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
        TimeWindow timeWindow = MessageFactory.getTimeWindow( Duration.ofHours( 1 ),
                                                              Duration.ofHours( 2 ) );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                               .setSeconds( 1800 ) )
                                       .build();
        TimeScaleOuter timeScaleOuter = TimeScaleOuter.of( timeScale );
        TimeWindowOuter timeWindowOuter = TimeWindowOuter.of( timeWindow );

        TimeWindowOuter actual = TimeWindowSlicer.adjustTimeWindowForTimeScale( timeWindowOuter, timeScaleOuter );
        TimeWindow expectedInner = MessageFactory.getTimeWindow( Duration.ofMinutes( 30 ),
                                                                 Duration.ofHours( 2 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );

        assertEquals( expected, actual );
    }

    @Test
    void testAdjustTimeWindowEarliestValidTimeForTimeScale()
    {
        Instant earliest = Instant.parse( "2055-03-23T00:00:00Z" );
        Instant latest = Instant.parse( "2055-03-24T00:00:00Z" );

        TimeWindow timeWindow = MessageFactory.getTimeWindow( earliest, latest );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                               .setSeconds( 86400 ) )
                                       .build();
        TimeScaleOuter timeScaleOuter = TimeScaleOuter.of( timeScale );
        TimeWindowOuter timeWindowOuter = TimeWindowOuter.of( timeWindow );

        TimeWindowOuter actual = TimeWindowSlicer.adjustTimeWindowForTimeScale( timeWindowOuter, timeScaleOuter );
        TimeWindow expectedInner = MessageFactory.getTimeWindow( Instant.parse( "2055-03-22T00:00:00Z" ),
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 1 ) )
                                                  .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 24 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 0 ),
                                                               Duration.ofHours( 1 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 1 ),
                                                               Duration.ofHours( 2 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 2 ),
                                                               Duration.ofHours( 3 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 3 ),
                                                               Duration.ofHours( 4 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 4 ),
                                                               Duration.ofHours( 5 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 5 ),
                                                               Duration.ofHours( 6 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 6 ),
                                                               Duration.ofHours( 7 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 7 ),
                                                               Duration.ofHours( 8 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 8 ),
                                                               Duration.ofHours( 9 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 9 ),
                                                               Duration.ofHours( 10 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 10 ),
                                                               Duration.ofHours( 11 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 11 ),
                                                               Duration.ofHours( 12 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 12 ),
                                                               Duration.ofHours( 13 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 13 ),
                                                               Duration.ofHours( 14 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 14 ),
                                                               Duration.ofHours( 15 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 15 ),
                                                               Duration.ofHours( 16 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 16 ),
                                                               Duration.ofHours( 17 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 17 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 18 ),
                                                               Duration.ofHours( 19 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 19 ),
                                                               Duration.ofHours( 20 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 20 ),
                                                               Duration.ofHours( 21 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 21 ),
                                                               Duration.ofHours( 22 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 22 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 23 ),
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 24 ) )
                                                  .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 2 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 0 ),
                                                               Duration.ofHours( 24 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 24 ),
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 23 ) )
                                                  .frequency( Duration.ofHours( 17 ) )
                                                  .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_NINE ) )
                                                         .maximum( Instant.parse( INSTANT_TEN ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 18 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                               //2551-03-17T00:00:00Z
                                                               Instant.parse( INSTANT_TWELVE ),
                                                               //2551-03-17T13:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                               //2551-03-17T00:00:00Z
                                                               Instant.parse( INSTANT_TWELVE ),
                                                               //2551-03-17T13:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                               //2551-03-17T07:00:00Z
                                                               Instant.parse( INSTANT_FOURTEEN ),
                                                               //2551-03-17T20:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                               //2551-03-17T07:00:00Z
                                                               Instant.parse( INSTANT_FOURTEEN ),
                                                               //2551-03-17T20:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                               //2551-03-17T14:00:00Z
                                                               Instant.parse( INSTANT_SIXTEEN ),
                                                               //2551-03-18T03:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                               //2551-03-17T14:00:00Z
                                                               Instant.parse( INSTANT_SIXTEEN ),
                                                               //2551-03-18T03:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                               //2551-03-17T21:00:00Z
                                                               Instant.parse( INSTANT_EIGHTEEN ),
                                                               //2551-03-18T10:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                               //2551-03-17T21:00:00Z
                                                               Instant.parse( INSTANT_EIGHTEEN ),
                                                               //2551-03-18T10:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                               //2551-03-18T04:00:00Z
                                                               Instant.parse( INSTANT_TWENTY ),
                                                               //2551-03-18T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                               //2551-03-18T04:00:00Z
                                                               Instant.parse( INSTANT_TWENTY ),
                                                               //2551-03-18T17:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                               //2551-03-18T11:00:00Z
                                                               Instant.parse( INSTANT_ELEVEN ),
                                                               //2551-03-19T00:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                               //2551-03-18T11:00:00Z
                                                               Instant.parse( INSTANT_ELEVEN ),
                                                               //2551-03-19T00:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                               //2551-03-18T18:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_THREE ),
                                                               //2551-03-19T07:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                               //2551-03-18T18:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_THREE ),
                                                               //2551-03-19T07:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                               //2551-03-19T01:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_FIVE ),
                                                               //2551-03-19T14:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                               //2551-03-19T01:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_FIVE ),
                                                               //2551-03-19T14:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
                                                               //2551-03-19T08:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_SEVEN ),
                                                               //2551-03-19T21:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 24 ) )
                                                  .build();
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
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
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
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.MIN,
                                                               Instant.MAX,
                                                               MessageFactory.DURATION_MIN,
                                                               MessageFactory.DURATION_MAX ) );

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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_ONE ) )
                                                         .maximum( Instant.parse( INSTANT_TWO ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 1 ) )
                                                       .frequency( Duration.ofHours( 1 ) )
                                                       .build();
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
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_FOUR ),
                                                               //2017-08-08T01:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                               //2017-08-08T01:00:00Z
                                                               Instant.parse( INSTANT_FIVE ),
                                                               //2017-08-08T02:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIVE ),
                                                               //2017-08-08T02:00:00Z
                                                               Instant.parse( INSTANT_SIX ),
                                                               //2017-08-08T03:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SIX ),
                                                               //2017-08-08T03:00:00Z
                                                               Instant.parse( INSTANT_SEVEN ),
                                                               //2017-08-08T04:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVEN ),
                                                               //2017-08-08T04:00:00Z
                                                               Instant.parse( INSTANT_EIGHT ),
                                                               //2017-08-08T05:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_EIGHT ),
                                                               //2017-08-08T05:00:00Z
                                                               Instant.parse( "2017-08-08T06:00:00Z" ),
                                                               //2017-08-08T06:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T06:00:00Z" ),
                                                               //2017-08-08T06:00:00Z
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               //2017-08-08T07:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               //2017-08-08T07:00:00Z
                                                               Instant.parse( "2017-08-08T08:00:00Z" ),
                                                               //2017-08-08T08:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T08:00:00Z" ),
                                                               //2017-08-08T08:00:00Z
                                                               Instant.parse( "2017-08-08T09:00:00Z" ),
                                                               //2017-08-08T09:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T09:00:00Z" ),
                                                               //2017-08-08T09:00:00Z
                                                               Instant.parse( "2017-08-08T10:00:00Z" ),
                                                               //2017-08-08T10:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T10:00:00Z" ),
                                                               //2017-08-08T10:00:00Z
                                                               Instant.parse( "2017-08-08T11:00:00Z" ),
                                                               //2017-08-08T11:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T11:00:00Z" ),
                                                               //2017-08-08T11:00:00Z
                                                               Instant.parse( "2017-08-08T12:00:00Z" ),
                                                               //2017-08-08T12:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T12:00:00Z" ),
                                                               //2017-08-08T12:00:00Z
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               //2017-08-08T13:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               //2017-08-08T13:00:00Z
                                                               Instant.parse( "2017-08-08T14:00:00Z" ),
                                                               //2017-08-08T14:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T14:00:00Z" ),
                                                               //2017-08-08T14:00:00Z
                                                               Instant.parse( "2017-08-08T15:00:00Z" ),
                                                               //2017-08-08T15:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T15:00:00Z" ),
                                                               //2017-08-08T15:00:00Z
                                                               Instant.parse( "2017-08-08T16:00:00Z" ),
                                                               //2017-08-08T16:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T16:00:00Z" ),
                                                               //2017-08-08T16:00:00Z
                                                               Instant.parse( "2017-08-08T17:00:00Z" ),
                                                               //2017-08-08T17:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T17:00:00Z" ),
                                                               //2017-08-08T17:00:00Z
                                                               Instant.parse( "2017-08-08T18:00:00Z" ),
                                                               //2017-08-08T18:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T18:00:00Z" ),
                                                               //2017-08-08T18:00:00Z
                                                               Instant.parse( "2017-08-08T19:00:00Z" ),
                                                               //2017-08-08T19:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T19:00:00Z" ),
                                                               //2017-08-08T19:00:00Z
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               //2017-08-08T20:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               //2017-08-08T20:00:00Z
                                                               Instant.parse( "2017-08-08T21:00:00Z" ),
                                                               //2017-08-08T21:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T21:00:00Z" ),
                                                               //2017-08-08T21:00:00Z
                                                               Instant.parse( "2017-08-08T22:00:00Z" ),
                                                               //2017-08-08T22:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T22:00:00Z" ),
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
        TimePools referenceTimePoolsNoFreq = TimePoolsBuilder.builder()
                                                             .period( Duration.ofHours( 1 ) )
                                                             .build();
        EvaluationDeclaration declarationNoFreq = EvaluationDeclarationBuilder.builder()
                                                                              .leadTimes( leadTimes )
                                                                              .leadTimePools( leadTimePools )
                                                                              .referenceDates( referenceDates )
                                                                              .referenceDatePools(
                                                                                      referenceTimePoolsNoFreq )
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
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
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
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 9 );

        Duration first = Duration.ofHours( 0 );
        Duration last = Duration.ofHours( 40 );

        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                               //2551-03-17T00:00:00Z
                                                               Instant.parse( INSTANT_TWELVE ),
                                                               //2551-03-17T13:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                               //2551-03-17T07:00:00Z
                                                               Instant.parse( INSTANT_FOURTEEN ),
                                                               //2551-03-17T20:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                               //2551-03-17T14:00:00Z
                                                               Instant.parse( INSTANT_SIXTEEN ),
                                                               //2551-03-18T03:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                               //2551-03-17T21:00:00Z
                                                               Instant.parse( INSTANT_EIGHTEEN ),
                                                               //2551-03-18T10:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                               //2551-03-18T04:00:00Z
                                                               Instant.parse( INSTANT_TWENTY ),
                                                               //2551-03-18T17:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                               //2551-03-18T11:00:00Z
                                                               Instant.parse( INSTANT_ELEVEN ),
                                                               //2551-03-19T00:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                               //2551-03-18T18:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_THREE ),
                                                               //2551-03-19T07:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                               //2551-03-19T01:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_FIVE ),
                                                               //2551-03-19T14:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 0 ) )
                                                  .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );

        Duration first = Duration.ofHours( 43 );
        Duration last = Duration.ofHours( 43 );

        TimeWindow inner = MessageFactory.getTimeWindow( first,
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
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
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

        TimeWindow innerOne = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                            Instant.parse( "2017-08-08T13:00:00Z" ) );
        TimeWindow innerTwo = MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T07:00:00Z" ),
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 8 ) )
                                                  .frequency( Duration.ofHours( 7 ) )
                                                  .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
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
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 26 ),
                                                               Duration.ofHours( 34 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 8 ) )
                                                  .frequency( Duration.ofHours( 7 ) )
                                                  .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_TWO ) )
                                                         .maximum( Instant.parse( INSTANT_THREE ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 17 ) )
                                                       .frequency( Duration.ofHours( 23 ) )
                                                       .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
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
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                               Instant.parse( "2017-08-09T16:00:00Z" ),
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                               Instant.parse( "2017-08-09T16:00:00Z" ),
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                               Instant.parse( "2017-08-09T16:00:00Z" ),
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 26 ),
                                                               Duration.ofHours( 34 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
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
    void testAggregateTimeWindowsWithMaximum()
    {
        Set<TimeWindowOuter> timeWindows = this.getTimeWindowsForAggregationTesting();
        TimeWindowOuter actual = TimeWindowSlicer.aggregate( timeWindows, TimeWindowAggregation.MAXIMUM );
        TimeWindow expectedInner = MessageFactory.getTimeWindow( Instant.parse( "2088-08-08T23:00:00Z" ),
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
        TimeWindow expectedInner = MessageFactory.getTimeWindow( Instant.parse( "2099-08-08T23:00:00Z" ),
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
        TimeWindow expectedInner = MessageFactory.getTimeWindow( Instant.parse( "2092-08-08T15:00:00Z" ),
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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();

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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();

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
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();

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
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
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
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();

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
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();

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
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
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
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
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
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
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
        timeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2099-08-08T23:00:00Z" ),
                                                       Instant.parse( "2109-08-09T16:00:00Z" ),
                                                       Instant.parse( "2090-08-07T13:00:00Z" ),
                                                       Instant.parse( "2100-08-08T13:00:00Z" ),
                                                       Duration.ofHours( 19 ),
                                                       Duration.ofHours( 27 ) ) );
        timeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2088-08-08T23:00:00Z" ),
                                                       Instant.parse( "2110-08-09T16:00:00Z" ),
                                                       Instant.parse( "2080-08-08T07:00:00Z" ),
                                                       Instant.parse( "2101-08-08T20:00:00Z" ),
                                                       Duration.ofHours( 13 ),
                                                       Duration.ofHours( 29 ) ) );
        timeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2089-08-08T23:00:00Z" ),
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

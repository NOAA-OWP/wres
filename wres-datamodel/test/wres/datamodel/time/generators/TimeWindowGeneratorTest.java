package wres.datamodel.time.generators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.DateCondition;
import wres.config.generated.DurationUnit;
import wres.config.generated.IntBoundsType;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.generators.TimeWindowGenerator;

/**
 * <p>Tests the {@link TimeWindowGenerator}.
 *
 * @author james.brown@hydrosolved.com
 */

public final class TimeWindowGeneratorTest
{

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
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)}
     * where the project declaration includes a <code>leadHours</code> and a 
     * <code>leadTimesPoolingWindow</code>. Expects twenty-four time windows with
     * prescribed characteristics.
     * 
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated 
     * with system test scenario017, as of commit fa548da9da85b16631f238f78b358d85ddbebed5.
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursAndLeadTimesPoolingWindowReturnsTwentyFourWindows()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 24 );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 1, null, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 24 );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 0 ), Duration.ofHours( 1 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 1 ), Duration.ofHours( 2 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 2 ), Duration.ofHours( 3 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 3 ), Duration.ofHours( 4 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 4 ), Duration.ofHours( 5 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 5 ), Duration.ofHours( 6 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 6 ), Duration.ofHours( 7 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 7 ), Duration.ofHours( 8 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 8 ), Duration.ofHours( 9 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 9 ), Duration.ofHours( 10 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 10 ), Duration.ofHours( 11 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 11 ), Duration.ofHours( 12 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 12 ), Duration.ofHours( 13 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 13 ), Duration.ofHours( 14 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 14 ), Duration.ofHours( 15 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 15 ), Duration.ofHours( 16 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 16 ), Duration.ofHours( 17 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 17 ), Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 18 ), Duration.ofHours( 19 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 19 ), Duration.ofHours( 20 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 20 ), Duration.ofHours( 21 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 21 ), Duration.ofHours( 22 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 22 ), Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 23 ), Duration.ofHours( 24 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 24, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)}
     * where the project declaration includes a <code>leadHours</code> and a 
     * <code>leadTimesPoolingWindow</code>. Expects two time windows with
     * prescribed characteristics.
     * 
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated 
     * with system test scenario403, as of commit fa548da9da85b16631f238f78b358d85ddbebed5.
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursAndLeadTimesPoolingWindowReturnsTwoWindows()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 48 );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 24, null, DurationUnit.HOURS );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 2 );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 0 ), Duration.ofHours( 24 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 24 ), Duration.ofHours( 48 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 2, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)}
     * where the project declaration includes a <code>leadHours</code>, a 
     * <code>leadTimesPoolingWindow</code>, an <code>issuedDates</code>, and 
     * an <code>issuedDatesPoolingWindow</code>. Expects eighteen time 
     * windows with prescribed characteristics.
     * 
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated 
     * with system test scenario505, which is in development as of commit 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c.
     */

    @Test
    public void
            testGetTimeWindowsWithLeadHoursIssuedDatesLeadTimesPoolingWindowAndIssuedDatesPoolingWindowReturnsEighteenWindows()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 40 );
        // (2551-03-17T00:00:00Z, 2551-03-20T00:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( INSTANT_NINE, INSTANT_TEN );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 23, 17, DurationUnit.HOURS );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 18 );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_NINE ), //2551-03-17T00:00:00Z
                                                Instant.parse( INSTANT_TWELVE ), //2551-03-17T13:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_NINE ), //2551-03-17T00:00:00Z
                                                Instant.parse( INSTANT_TWELVE ), //2551-03-17T13:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_THIRTEEN ), //2551-03-17T07:00:00Z
                                                Instant.parse( INSTANT_FOURTEEN ), //2551-03-17T20:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_THIRTEEN ), //2551-03-17T07:00:00Z
                                                Instant.parse( INSTANT_FOURTEEN ), //2551-03-17T20:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_FIFTEEN ), //2551-03-17T14:00:00Z
                                                Instant.parse( INSTANT_SIXTEEN ), //2551-03-18T03:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_FIFTEEN ), //2551-03-17T14:00:00Z
                                                Instant.parse( INSTANT_SIXTEEN ), //2551-03-18T03:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_SEVENTEEN ), //2551-03-17T21:00:00Z
                                                Instant.parse( INSTANT_EIGHTEEN ), //2551-03-18T10:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_SEVENTEEN ), //2551-03-17T21:00:00Z
                                                Instant.parse( INSTANT_EIGHTEEN ), //2551-03-18T10:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_NINETEEN ), //2551-03-18T04:00:00Z
                                                Instant.parse( INSTANT_TWENTY ), //2551-03-18T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_NINETEEN ), //2551-03-18T04:00:00Z
                                                Instant.parse( INSTANT_TWENTY ), //2551-03-18T17:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_ONE ), //2551-03-18T11:00:00Z
                                                Instant.parse( INSTANT_ELEVEN ), //2551-03-19T00:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_ONE ), //2551-03-18T11:00:00Z
                                                Instant.parse( INSTANT_ELEVEN ), //2551-03-19T00:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_TWO ), //2551-03-18T18:00:00Z
                                                Instant.parse( INSTANT_TWENTY_THREE ), //2551-03-19T07:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_TWO ), //2551-03-18T18:00:00Z
                                                Instant.parse( INSTANT_TWENTY_THREE ), //2551-03-19T07:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_FOUR ), //2551-03-19T01:00:00Z
                                                Instant.parse( INSTANT_TWENTY_FIVE ), //2551-03-19T14:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_FOUR ), //2551-03-19T01:00:00Z
                                                Instant.parse( INSTANT_TWENTY_FIVE ), //2551-03-19T14:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_SIX ), //2551-03-19T08:00:00Z
                                                Instant.parse( INSTANT_TWENTY_SEVEN ), //2551-03-19T21:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_SIX ), //2551-03-19T08:00:00Z
                                                Instant.parse( INSTANT_TWENTY_SEVEN ), //2551-03-19T21:00:00Z
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 18, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)}
     * where the project declaration includes a <code>leadHours</code>, a 
     * <code>leadTimesPoolingWindow</code>, a <code>dates</code> and an 
     * <code>issuedDates</code>. Expects one time window with prescribed characteristics.
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursDatesIssuedDatesAndLeadTimesPoolingWindowReturnsOneWindow()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 1, 48 );
        // (2551-03-17T00:00:00Z, 2551-03-20T00:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( INSTANT_NINE, INSTANT_TEN );
        // (2551-03-19T00:00:00Z, 2551-03-24T00:00:00Z)
        DateCondition datesConfig = new DateCondition( INSTANT_ELEVEN, INSTANT_TWENTY_EIGHT );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 24, null, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 datesConfig,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_NINE ), //2551-03-17T00:00:00Z
                                                Instant.parse( INSTANT_TEN ), //2551-03-20T00:00:00Z
                                                Instant.parse( INSTANT_ELEVEN ), //2551-03-19T00:00:00Z
                                                Instant.parse( INSTANT_TWENTY_EIGHT ), //2551-03-24T00:00:00Z
                                                Duration.ofHours( 1 ),
                                                Duration.ofHours( 25 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} where the project declaration 
     * does not include any constraints on time, aka "one big pool".
     * 
     * <p>This is analogous to system test scenario508, as of commit b9a7214ec22999482784119a8527149348c80119.
     */

    @Test
    public void testGetTimeWindowsFromUnconstrainedPairConfigReturnsOneWindow()
    {
        // Mock the sufficient elements of the ProjectConfig
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( TimeWindow.of( Instant.MIN,
                                                Instant.MAX,
                                                Instant.MIN,
                                                Instant.MAX,
                                                TimeWindow.DURATION_MIN,
                                                TimeWindow.DURATION_MAX ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)}
     * where the project declaration includes a <code>issuedDatesPoolingWindow</code> and a 
     * <code>leadTimesPoolingWindow</code>. Expects twenty-three time windows. Tests both
     * an explicit and implicit declaration of the <code>frequency</code>.
     * 
     * <p>The project declaration from this test matches the declaration associated 
     * with system test scenario704, as of commit da07c16148429740496b8cc6df89a73e3697f17c, 
     * except the <code>period</code> is 1.0 time units here.
     */

    @Test
    public void
            testGetTimeWindowsWithLeadHoursDatesIssuedDatesIssuedDatesPoolingWindowAndLeadTimesPoolingWindowReturnsTwentyThreeWindows()
    {
        // Mock the sufficient elements of the ProjectConfig

        // Lead durations for all time windows
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 18 );

        // Issued dates into which all time windows must fit
        // (2017-08-08T00:00:00Z, 2017-08-08T23:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( INSTANT_ONE, INSTANT_TWO );

        // Valid dates into which all time windows must fit
        // (2017-08-08T00:00:00Z, 2017-08-09T17:00:00Z)
        DateCondition datesConfig = new DateCondition( INSTANT_ONE, INSTANT_THREE );

        // The declaration of the time windows by lead duration and issued date
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 18, null, DurationUnit.HOURS );

        // Use a period that equals the frequency
        // This allows for the testing of an explicit and implicit declaration of the frequency
        // in one test scenario, as the default behavior, when omitting the frequency, is frequency=period
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 1, 1, DurationUnit.HOURS );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 datesConfig,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        // Generate the expected time windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 23 );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_FOUR ), //2017-08-08T01:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_FOUR ), //2017-08-08T01:00:00Z
                                                Instant.parse( INSTANT_FIVE ), //2017-08-08T02:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_FIVE ), //2017-08-08T02:00:00Z
                                                Instant.parse( INSTANT_SIX ), //2017-08-08T03:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_SIX ), //2017-08-08T03:00:00Z
                                                Instant.parse( INSTANT_SEVEN ), //2017-08-08T04:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_SEVEN ), //2017-08-08T04:00:00Z
                                                Instant.parse( INSTANT_EIGHT ), //2017-08-08T05:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_EIGHT ), //2017-08-08T05:00:00Z
                                                Instant.parse( "2017-08-08T06:00:00Z" ), //2017-08-08T06:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T06:00:00Z" ), //2017-08-08T06:00:00Z
                                                Instant.parse( "2017-08-08T07:00:00Z" ), //2017-08-08T07:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T07:00:00Z" ), //2017-08-08T07:00:00Z
                                                Instant.parse( "2017-08-08T08:00:00Z" ), //2017-08-08T08:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T08:00:00Z" ), //2017-08-08T08:00:00Z
                                                Instant.parse( "2017-08-08T09:00:00Z" ), //2017-08-08T09:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T09:00:00Z" ), //2017-08-08T09:00:00Z
                                                Instant.parse( "2017-08-08T10:00:00Z" ), //2017-08-08T10:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T10:00:00Z" ), //2017-08-08T10:00:00Z
                                                Instant.parse( "2017-08-08T11:00:00Z" ), //2017-08-08T11:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T11:00:00Z" ), //2017-08-08T11:00:00Z
                                                Instant.parse( "2017-08-08T12:00:00Z" ), //2017-08-08T12:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T12:00:00Z" ), //2017-08-08T12:00:00Z
                                                Instant.parse( "2017-08-08T13:00:00Z" ), //2017-08-08T13:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T13:00:00Z" ), //2017-08-08T13:00:00Z
                                                Instant.parse( "2017-08-08T14:00:00Z" ), //2017-08-08T14:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T14:00:00Z" ), //2017-08-08T14:00:00Z
                                                Instant.parse( "2017-08-08T15:00:00Z" ), //2017-08-08T15:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T15:00:00Z" ), //2017-08-08T15:00:00Z
                                                Instant.parse( "2017-08-08T16:00:00Z" ), //2017-08-08T16:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T16:00:00Z" ), //2017-08-08T16:00:00Z
                                                Instant.parse( "2017-08-08T17:00:00Z" ), //2017-08-08T17:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T17:00:00Z" ), //2017-08-08T17:00:00Z
                                                Instant.parse( "2017-08-08T18:00:00Z" ), //2017-08-08T18:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T18:00:00Z" ), //2017-08-08T18:00:00Z
                                                Instant.parse( "2017-08-08T19:00:00Z" ), //2017-08-08T19:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T19:00:00Z" ), //2017-08-08T19:00:00Z
                                                Instant.parse( "2017-08-08T20:00:00Z" ), //2017-08-08T20:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T20:00:00Z" ), //2017-08-08T20:00:00Z
                                                Instant.parse( "2017-08-08T21:00:00Z" ), //2017-08-08T21:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T21:00:00Z" ), //2017-08-08T21:00:00Z
                                                Instant.parse( "2017-08-08T22:00:00Z" ), //2017-08-08T22:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2017-08-08T22:00:00Z" ), //2017-08-08T22:00:00Z
                                                Instant.parse( INSTANT_TWO ), //2017-08-08T23:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );

        // Generate the actual time windows for the explicit test
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 23, actualTimeWindows.size() );

        // Assert that the expected and actual time windows are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );

        // Declare the same version of this test with implicit frequency
        PoolingWindowConfig issuedDatesPoolingWindowConfigNoFreq =
                new PoolingWindowConfig( 1, null, DurationUnit.HOURS );

        PairConfig pairsConfigNoFreq = new PairConfig( null,
                                                       null,
                                                       null,
                                                       leadBoundsConfig,
                                                       null,
                                                       datesConfig,
                                                       issuedDatesConfig,
                                                       null,
                                                       null,
                                                       null,
                                                       issuedDatesPoolingWindowConfigNoFreq,
                                                       leadTimesPoolingWindowConfig,
                                                       null,
                                                       null );

        // Generate the actual time windows for the implicit test
        Set<TimeWindow> actualTimeWindowsNoFreq = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfigNoFreq );

        // Assert that the expected and actual time windows are equal
        assertEquals( expectedTimeWindows, actualTimeWindowsNoFreq );
    }

    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} where the project declaration 
     * does not include any explicit time windows, but is constrained by <code>leadHours</code>, 
     * <code>issuedDates</code> and <code>dates</code>, aka "one big pool" with constraints.
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursDatesAndIssuedDatesReturnsOneWindow()
    {
        // Mock the sufficient elements of the ProjectConfig
        // Lead durations for all time windows
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 18 );

        // Issued dates into which all time windows must fit
        // (2017-08-08T00:00:00Z, 2017-08-08T23:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( INSTANT_ONE, INSTANT_TWO );

        // Valid dates into which all time windows must fit
        // (2017-08-08T00:00:00Z, 2017-08-09T17:00:00Z)
        DateCondition datesConfig = new DateCondition( INSTANT_ONE, INSTANT_THREE );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 datesConfig,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_TWO ), //2017-08-08T23:00:00Z
                                                Instant.parse( INSTANT_ONE ), //2017-08-08T00:00:00Z
                                                Instant.parse( INSTANT_THREE ), //2017-08-09T17:00:00Z
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 18 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)}
     * where the project declaration includes a <code>leadHours</code>, a 
     * an <code>issuedDates</code>, and an <code>issuedDatesPoolingWindow</code>. 
     * Expects nine time windows with prescribed characteristics.
     * 
     * <p>The project declaration from this test scenario is similar to the declaration associated 
     * with system test scenario505, as of commit c8def0cf2d608c0617786f7cb4f28b563960d667, but without
     * a <code>leadTimesPoolingWindow</code>.
     */

    @Test
    public void
            testGetTimeWindowsWithLeadHoursIssuedDatesAndIssuedDatesPoolingWindowAndNoLeadTimesPoolingWindowReturnsNineWindows()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 40 );
        // (2551-03-17T00:00:00Z, 2551-03-20T00:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( INSTANT_NINE, INSTANT_TEN );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 null,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 9 );

        Duration first = Duration.ofHours( 0 );
        Duration last = Duration.ofHours( 40 );

        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_NINE ), //2551-03-17T00:00:00Z
                                                Instant.parse( INSTANT_TWELVE ), //2551-03-17T13:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_THIRTEEN ), //2551-03-17T07:00:00Z
                                                Instant.parse( INSTANT_FOURTEEN ), //2551-03-17T20:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_FIFTEEN ), //2551-03-17T14:00:00Z
                                                Instant.parse( INSTANT_SIXTEEN ), //2551-03-18T03:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_SEVENTEEN ), //2551-03-17T21:00:00Z
                                                Instant.parse( INSTANT_EIGHTEEN ), //2551-03-18T10:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_NINETEEN ), //2551-03-18T04:00:00Z
                                                Instant.parse( INSTANT_TWENTY ), //2551-03-18T17:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_ONE ), //2551-03-18T11:00:00Z
                                                Instant.parse( INSTANT_ELEVEN ), //2551-03-19T00:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_TWO ), //2551-03-18T18:00:00Z
                                                Instant.parse( INSTANT_TWENTY_THREE ), //2551-03-19T07:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_FOUR ), //2551-03-19T01:00:00Z
                                                Instant.parse( INSTANT_TWENTY_FIVE ), //2551-03-19T14:00:00Z
                                                first,
                                                last ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( INSTANT_TWENTY_SIX ), //2551-03-19T08:00:00Z
                                                Instant.parse( INSTANT_TWENTY_SEVEN ), //2551-03-19T21:00:00Z
                                                first,
                                                last ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 9, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }
    
    /**
     * <p>Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)}
     * where the project declaration includes a <code>leadHours</code> and an <code>leadTimesPoolingWindow</code> and 
     * the <code>minimum</code> and <code>maximum</code> lead hours are the same value and the period associated with 
     * the <code>leadTimesPoolingWindow</code> is zero wide. This is equivalent to system test scenario010 as of 
     * commit 8480aa4d4ddc09275746fe590623ecfd83e452ae and is used to check that a zero-wide pool centered on a
     * single lead duration does not increment infinitely.
     */

    @Test
    public void testGetTimeWindowsWithZeroWideLeadHoursAndLeadTimesPoolingWindowWithZeroPeriodReturnsOneWindows()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 43, 43 );
        
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 0, null, DurationUnit.HOURS );
        
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );

        Duration first = Duration.ofHours( 43 );
        Duration last = Duration.ofHours( 43 );

        expectedTimeWindows.add( TimeWindow.of( first,
                                                last ) );
        
        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }    

    /**
     * Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} for an expected exception when 
     * <code>leadHours</code> are required but missing.
     */

    @Test
    public void testGetTimeWindowsThrowsProjectConfigExceptionWhenLeadHoursExpectedButMissing()
    {
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 18, null, DurationUnit.HOURS );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        ProjectConfigException thrown = assertThrows( ProjectConfigException.class,
                                                      () -> TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig ) );

        assertEquals( "Cannot determine lead duration time windows without a leadHours.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} for an expected exception when the 
     * <code>minimum</code> <code>leadHours</code> is required but missing.
     */

    @Test
    public void testGetTimeWindowsThrowsProjectConfigExceptionWhenLeadHoursMinimumExpectedButMissing()
    {
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 18, null, DurationUnit.HOURS );

        IntBoundsType leadBoundsConfig = new IntBoundsType( null, 40 );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        ProjectConfigException thrown = assertThrows( ProjectConfigException.class,
                                                      () -> TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig ) );

        assertEquals( "Cannot determine lead duration time windows without a minimum leadHours.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} for an expected exception when the 
     * <code>maximum</code> <code>leadHours</code> is required but missing.
     */

    @Test
    public void testGetTimeWindowsThrowsProjectConfigExceptionWhenLeadHoursMaximumExpectedButMissing()
    {
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 18, null, DurationUnit.HOURS );

        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, null );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );

        ProjectConfigException thrown = assertThrows( ProjectConfigException.class,
                                                      () -> TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig ) );

        assertEquals( "Cannot determine lead duration time windows without a maximum leadHours.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} for an expected exception when 
     * <code>issuedDates</code> is required but missing.
     */

    @Test
    public void testGetTimeWindowsThrowsProjectConfigExceptionWhenIssuedDatesExpectedButMissing()
    {
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 null,
                                                 null,
                                                 null );

        ProjectConfigException thrown = assertThrows( ProjectConfigException.class,
                                                      () -> TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig ) );

        assertEquals( "Cannot determine issued dates time windows without an issuedDates.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} for an expected exception when the 
     * <code>earliest</code> <code>issuedDates</code> is required but missing.
     */

    @Test
    public void testGetTimeWindowsThrowsProjectConfigExceptionWhenIssuedDatesEarliestExpectedButMissing()
    {
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );

        DateCondition issuedDatesConfig = new DateCondition( null, INSTANT_ONE );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 null,
                                                 null,
                                                 null );

        ProjectConfigException thrown = assertThrows( ProjectConfigException.class,
                                                      () -> TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig ) );

        assertEquals( "Cannot determine issued dates time windows without an earliest issuedDates.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link TimeWindowGenerator#getTimeWindowsFromPairConfig(PairConfig)} for an expected exception when 
     * the <code>latest</code> <code>issuedDates</code> is required but missing.
     */

    @Test
    public void testGetTimeWindowsThrowsProjectConfigExceptionWhenIssuedDatesLatestExpectedButMissing()
    {
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );

        DateCondition issuedDatesConfig = new DateCondition( INSTANT_ONE, null );

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 null,
                                                 null,
                                                 null );

        ProjectConfigException thrown = assertThrows( ProjectConfigException.class,
                                                      () -> TimeWindowGenerator.getTimeWindowsFromPairConfig( pairsConfig ) );

        assertEquals( "Cannot determine issued dates time windows without a latest issuedDates.",
                      thrown.getMessage() );
    }

}

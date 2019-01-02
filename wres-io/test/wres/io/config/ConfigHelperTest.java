package wres.io.config;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DateCondition;
import wres.config.generated.DurationUnit;
import wres.config.generated.IntBoundsType;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.metadata.TimeWindow;

@RunWith( PowerMockRunner.class )
@PowerMockIgnore( "javax.management.*" )
public class ConfigHelperTest
{
    @Test
    public void getTimeshift()
    {
        DataSourceConfig.TimeShift configuredTimeShift = new DataSourceConfig.TimeShift(
                                                                                         2,
                                                                                         DurationUnit.HOURS );

        DataSourceConfig dataSourceConfig = new DataSourceConfig(
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  configuredTimeShift,
                                                                  null,
                                                                  null );

        Duration timeShift = ConfigHelper.getTimeShift( dataSourceConfig );
        Duration controlTimeShift = Duration.of( 2, ChronoUnit.HOURS );

        Assert.assertEquals( "The correct configured timeshift was not created.", controlTimeShift, timeShift );
    }

    @Test
    public void getNullTimeshift()
    {
        DataSourceConfig dataSourceConfig = new DataSourceConfig(
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null );

        Duration timeshift = ConfigHelper.getTimeShift( dataSourceConfig );

        Assert.assertEquals( "A null timeshift was not created.", null, timeshift );

    }

    /**
     * <p>Tests the {@link ConfigHelper#getTimeWindowsFromProjectConfig(wres.config.generated.ProjectConfig)}
     * where the project declaration includes a <code>leadHours</code> and a 
     * <code>leadTimesPoolingWindow</code>. Expects twenty-four time windows with
     * prescribed characteristics.
     * 
     * <p>This test scenario is analogous to system test scenario017 as of commit 
     * 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursAndLeadTimesPoolingWindowReturnsTwentyThreeWindows()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 1, 24 );
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
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   pairsConfig,
                                   null,
                                   null,
                                   null,
                                   null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 23 );
        for ( int i = 1; i < 24; i++ )
        {
            expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( i ), Duration.ofHours( i + 1 ) ) );
        }

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = ConfigHelper.getTimeWindowsFromProjectConfig( mockedConfig );

        // Assert the expected cardinality
        assertEquals( 23, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link ConfigHelper#getTimeWindowsFromProjectConfig(wres.config.generated.ProjectConfig)}
     * where the project declaration includes a <code>leadHours</code> and a 
     * <code>leadTimesPoolingWindow</code>. Expects one time windows with
     * prescribed characteristics.
     * 
     * <p>This test scenario is analogous to system test scenario403 as of commit 
     * 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursAndLeadTimesPoolingWindowReturnsOneWindow()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 1, 48 );
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
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   pairsConfig,
                                   null,
                                   null,
                                   null,
                                   null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 1 ), Duration.ofHours( 25 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = ConfigHelper.getTimeWindowsFromProjectConfig( mockedConfig );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link ConfigHelper#getTimeWindowsFromProjectConfig(wres.config.generated.ProjectConfig)}
     * where the project declaration includes a <code>leadHours</code>, a 
     * <code>leadTimesPoolingWindow</code>, an <code>issuedDates</code>, and 
     * an <code>issuedDatesPoolingWindow</code>. Expects eighteen time 
     * windows with prescribed characteristics.
     * 
     * <p>This test scenario is analogous to system test scenario505 as of commit 
     * 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c
     */

    @Test
    public void
            testGetTimeWindowsWithLeadHoursIssuedDatesLeadTimesPoolingWindowAndIssuedDatesPoolingWindowReturnsEighteenWindows()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 40 );
        DateCondition issuedDatesConfig = new DateCondition( "2551-03-17T00:00:00Z", "2551-03-20T00:00:00Z" );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 23, 17, DurationUnit.HOURS );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   pairsConfig,
                                   null,
                                   null,
                                   null,
                                   null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 22 );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                Instant.parse( "2551-03-17T13:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                Instant.parse( "2551-03-17T13:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T07:00:00Z" ),
                                                Instant.parse( "2551-03-17T20:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T07:00:00Z" ),
                                                Instant.parse( "2551-03-17T20:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T14:00:00Z" ),
                                                Instant.parse( "2551-03-18T03:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T14:00:00Z" ),
                                                Instant.parse( "2551-03-18T03:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T21:00:00Z" ),
                                                Instant.parse( "2551-03-18T10:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T21:00:00Z" ),
                                                Instant.parse( "2551-03-18T10:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-18T04:00:00Z" ),
                                                Instant.parse( "2551-03-18T17:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-18T04:00:00Z" ),
                                                Instant.parse( "2551-03-18T17:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-18T11:00:00Z" ),
                                                Instant.parse( "2551-03-19T00:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-18T11:00:00Z" ),
                                                Instant.parse( "2551-03-19T00:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-18T18:00:00Z" ),
                                                Instant.parse( "2551-03-19T07:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-18T18:00:00Z" ),
                                                Instant.parse( "2551-03-19T07:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T01:00:00Z" ),
                                                Instant.parse( "2551-03-19T14:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T01:00:00Z" ),
                                                Instant.parse( "2551-03-19T14:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T08:00:00Z" ),
                                                Instant.parse( "2551-03-19T21:00:00Z" ),
                                                Duration.ofHours( 0 ),
                                                Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T08:00:00Z" ),
                                                Instant.parse( "2551-03-19T21:00:00Z" ),
                                                Duration.ofHours( 17 ),
                                                Duration.ofHours( 40 ) ) );

// Add these if the rule is containment of the left bookend only        
//        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T15:00:00Z" ),
//                                                Instant.parse( "2551-03-20T04:00:00Z" ),
//                                                Duration.ofHours( 0 ),
//                                                Duration.ofHours( 23 ) ) );
//        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T15:00:00Z" ),
//                                                Instant.parse( "2551-03-20T04:00:00Z" ),
//                                                Duration.ofHours( 17 ),
//                                                Duration.ofHours( 40 ) ) );
//        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T22:00:00Z" ),
//                                                Instant.parse( "2551-03-20T11:00:00Z" ),
//                                                Duration.ofHours( 0 ),
//                                                Duration.ofHours( 23 ) ) );
//        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-19T22:00:00Z" ),
//                                                Instant.parse( "2551-03-20T11:00:00Z" ),
//                                                Duration.ofHours( 17 ),
//                                                Duration.ofHours( 40 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = ConfigHelper.getTimeWindowsFromProjectConfig( mockedConfig );

        // Assert the expected cardinality
        assertEquals( 18, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link ConfigHelper#getTimeWindowsFromProjectConfig(wres.config.generated.ProjectConfig)}
     * where the project declaration includes a <code>leadHours</code>, a 
     * <code>leadTimesPoolingWindow</code>, a <code>dates</code> and a 
     * <code>issuedDates</code>. Expects one time window with prescribed characteristics.
     * 
     * <p>This test scenario is an extension of system test scenario403 as of commit 
     * 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursDatesIssuedDatesAndLeadTimesPoolingWindowReturnsOneWindow()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 1, 48 );
        DateCondition issuedDatesConfig = new DateCondition( "2551-03-17T00:00:00Z", "2551-03-20T00:00:00Z" );
        DateCondition datesConfig = new DateCondition( "2551-03-19T00:00:00Z", "2551-03-24T00:00:00Z" );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 24, null, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 datesConfig,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   pairsConfig,
                                   null,
                                   null,
                                   null,
                                   null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( TimeWindow.of( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                Instant.parse( "2551-03-20T00:00:00Z" ),
                                                Instant.parse( "2551-03-19T00:00:00Z" ),
                                                Instant.parse( "2551-03-24T00:00:00Z" ),
                                                Duration.ofHours( 1 ),
                                                Duration.ofHours( 25 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = ConfigHelper.getTimeWindowsFromProjectConfig( mockedConfig );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link ConfigHelper#getTimeWindowsFromProjectConfig(wres.config.generated.ProjectConfig)}
     * where the project declaration includes a <code>timeSeriesMetric</code>. Expects one time window that 
     * is unbounded in all dimensions.
     * 
     * <p>This test scenario is analogous to system test scenario1000 as of commit 
     * 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c
     */

    @Test
    public void testGetTimeWindowsWithTimeSeriesMetricReturnsOneWindow()
    {
        // Mock the sufficient elements of the ProjectConfig

        List<TimeSeriesMetricConfig> timeMetrics = new ArrayList<>();
        
        timeMetrics.add( new TimeSeriesMetricConfig( null,
                                                     null,
                                                     TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR,
                                                     null ) );
        
        List<MetricsConfig> metrics = Arrays.asList( new MetricsConfig( null, null, timeMetrics ) );

        ProjectConfig mockedConfig = new ProjectConfig( null,
                                                        new PairConfig( null,
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
                                                                        null ),
                                                        metrics,
                                                        null,
                                                        null,
                                                        null );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( TimeWindow.of( Instant.MIN,
                                                Instant.MAX,
                                                Instant.MIN,
                                                Instant.MAX,
                                                Duration.ofSeconds( Long.MIN_VALUE, 0 ),
                                                Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = ConfigHelper.getTimeWindowsFromProjectConfig( mockedConfig );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }
    
}

package wres.io.config;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.IntBoundsType;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
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
    public void testGetTimeWindowsWithLeadHoursAndLeadTimesPoolingWindowReturnsTwentyFourWindows()
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
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 24 );
        for ( int i = 0; i < 24; i++ )
        {
            expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( i ), Duration.ofHours( i + 1 ) ) );
        }

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = ConfigHelper.getTimeWindowsFromProjectConfig( mockedConfig );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link ConfigHelper#getTimeWindowsFromProjectConfig(wres.config.generated.ProjectConfig)}
     * where the project declaration includes a <code>leadHours</code> and a 
     * <code>leadTimesPoolingWindow</code>. Expects two time windows with
     * prescribed characteristics.
     * 
     * <p>This test scenario is analogous to system test scenario403 as of commit 
     * 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c
     */

    @Test
    public void testGetTimeWindowsWithLeadHoursAndLeadTimesPoolingWindowReturnsTwoWindows()
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
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 2 );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 0 ), Duration.ofHours( 24 ) ) );
        expectedTimeWindows.add( TimeWindow.of( Duration.ofHours( 24 ), Duration.ofHours( 48 ) ) );
        
        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = ConfigHelper.getTimeWindowsFromProjectConfig( mockedConfig );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    
}

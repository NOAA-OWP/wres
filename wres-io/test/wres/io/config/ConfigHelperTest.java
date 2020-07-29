package wres.io.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.junit.Assert;
import org.junit.Test;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DurationUnit;

public class ConfigHelperTest
{
    @Test
    public void getTimeshift()
    {
        DataSourceConfig.TimeShift configuredTimeShift = new DataSourceConfig.TimeShift(
                                                                                         2,
                                                                                         DurationUnit.HOURS );

        DataSourceConfig dataSourceConfig = new DataSourceConfig( null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  configuredTimeShift,
                                                                  null,
                                                                  null,
                                                                  null );

        Duration timeShift = ConfigHelper.getTimeShift( dataSourceConfig );
        Duration controlTimeShift = Duration.of( 2, ChronoUnit.HOURS );

        Assert.assertEquals( "The correct configured timeshift was not created.", controlTimeShift, timeShift );
    }

    @Test
    public void getNullTimeshift()
    {
        DataSourceConfig dataSourceConfig = new DataSourceConfig( null,
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
    

}

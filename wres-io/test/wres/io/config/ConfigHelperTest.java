package wres.io.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DataSourceConfig.Source;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.DatasourceType;
import wres.config.generated.DurationUnit;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;

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
                                                                  null );

        Duration timeshift = ConfigHelper.getTimeShift( dataSourceConfig );

        Assert.assertEquals( "A null timeshift was not created.", null, timeshift );

    }
    
    @Test
    public void testGetLeftOrRightOrBaselineWhenLeftAndRightDataSourceConfigAreEqual()
    {
        
        DataSourceConfig left = new DataSourceConfig( DatasourceType.OBSERVATIONS,
                                                      List.of( new Source( null,
                                                                           Format.USGS,
                                                                           null,
                                                                           null,
                                                                           null,
                                                                           null,
                                                                           false,
                                                                           null ) ),
                                                      new Variable( null, null, "ft3/s" ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      "USGS" );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.OBSERVATIONS,
                                                       List.of( new Source( null,
                                                                            Format.USGS,
                                                                            null,
                                                                            null,
                                                                            null,
                                                                            null,
                                                                            false,
                                                                            null ) ),
                                                       new Variable( null, null, "ft3/s" ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       "USGS obs" );

        Inputs inputs = new Inputs( left, right, null );

        ProjectConfig mockedConfig = new ProjectConfig( inputs,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null );

        System.out.println( left.hashCode());
        System.out.println( right.hashCode());
        
        
        LeftOrRightOrBaseline expected = ConfigHelper.getLeftOrRightOrBaseline( mockedConfig, right );
        
    }
    
    

}

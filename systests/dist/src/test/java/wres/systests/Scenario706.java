package wres.systests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.control.Control;
public class Scenario706
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario706.class );
    private static final String NEWLINE = System.lineSeparator();

    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "pairs.csv" ),
                    Path.of( "NWM_Short_Range_20170807T230000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T000000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T010000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T020000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T030000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T040000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T050000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T060000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T070000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T080000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T090000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T100000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T110000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T120000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T130000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T140000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T150000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T160000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T170000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T180000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T190000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T200000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T210000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T220000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T230000Z_64800_SECONDS.nc" ) );

    
    private ScenarioInformation scenarioInfo;
    
    /**
     * Watch for any failed assertions and log them.
     */

    @Rule
    public TestWatcher watcher = new TestWatcher()
    {
        @Override
        protected void failed( Throwable e, Description description )
        {
            LOGGER.error( description.toString(), e );
        }
    };

    @Before
    public void beforeIndividualTest() throws IOException, SQLException
    {
        LOGGER.info( "########################################################## EXECUTING "
                     + this.getClass().getSimpleName().toLowerCase()
                     + NEWLINE );
        this.scenarioInfo = new ScenarioInformation( this.getClass()
                                              .getSimpleName()
                                              .toLowerCase(),
                                              ScenarioHelper.getBaseDirectory() );
        ScenarioHelper.logUsedSystemProperties( scenarioInfo );
    }

    @Test
    public void testScenario()
    {
        Control control = ScenarioHelper.assertExecuteScenario( scenarioInfo );
        
        // Collect the file names actually written and that exist
        Set<Path> pathsWritten = control.get();
        Set<Path> actualFileNamesThatExist = pathsWritten.stream()
                                                         .filter( Files::exists )
                                                         .map( Path::getFileName )
                                                         .collect( Collectors.toSet() );

        // Expected file-name paths equals actual
        LOGGER.info( "Checking expected file names against actual file names that exist for {} files...",
                     EXPECTED_FILE_NAMES.size() );

        assertEquals( "The actual set of file names does not match the expected set of file names.",
                      EXPECTED_FILE_NAMES,
                      actualFileNamesThatExist );
        
        LOGGER.info( "Finished checking file names. The actual file names match the expected file names." );
        
        ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo, control );
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}


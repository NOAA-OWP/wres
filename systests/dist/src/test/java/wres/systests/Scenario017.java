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
public class Scenario017
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario017.class );
    private static final String NEWLINE = System.lineSeparator();

    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "DRRC2HSF_DRRC2HSF_HEFS_BRIER_SCORE.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_BRIER_SCORE.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_MEAN_ERROR.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_MEAN_ERROR.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_10800_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_10800_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_14400_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_14400_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_18000_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_18000_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_21600_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_21600_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_25200_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_25200_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_28800_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_28800_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_32400_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_32400_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_36000_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_36000_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_3600_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_3600_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_39600_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_39600_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_43200_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_43200_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_46800_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_46800_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_50400_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_50400_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_54000_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_54000_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_57600_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_57600_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_61200_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_61200_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_64800_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_64800_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_68400_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_68400_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_72000_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_72000_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_7200_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_7200_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_75600_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_75600_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_79200_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_79200_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_82800_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_82800_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.png" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_SAMPLE_SIZE.csv" ),
                    Path.of( "DRRC2HSF_DRRC2HSF_HEFS_SAMPLE_SIZE.png" ),
                    Path.of( "pairs.csv" ) );
    
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
        Set<Path> pathsWritten = ScenarioHelper.executeScenario( scenarioInfo );
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
        
        ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo, pathsWritten );
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}


package wres.systests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.control.Control;
public class Scenario300
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario300.class );
    private static final String NEWLINE = System.lineSeparator();

    private ScenarioInformation scenarioInfo;
    
    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "LGNN5_MAP_HEFS_BRIER_SCORE.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_BRIER_SCORE.png" ),
                    Path.of( "LGNN5_MAP_HEFS_MEAN_ERROR.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_MEAN_ERROR.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_108000_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_108000_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_129600_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_129600_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_151200_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_172800_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_172800_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_21600_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_21600_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_43200_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_43200_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_64800_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_64800_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.png" ),
                    Path.of( "LGNN5_MAP_HEFS_SAMPLE_SIZE.csv" ),
                    Path.of( "LGNN5_MAP_HEFS_SAMPLE_SIZE.png" ),
                    Path.of( "pairs.csv" ) );

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


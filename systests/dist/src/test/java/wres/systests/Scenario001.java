package wres.systests;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

import wres.control.Control;
public class Scenario001
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario001.class );
    private static final String NEWLINE = System.lineSeparator();

    private ScenarioInformation scenarioInfo;
    
    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "DRRC2_QINE_HEFS_BRIER_SCORE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BRIER_SCORE.png" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_ERROR.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_ERROR.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_10800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_10800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_14400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_14400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_18000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_18000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_21600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_21600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_25200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_25200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_28800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_28800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_32400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_32400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_36000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_36000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_3600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_3600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_39600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_39600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_43200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_43200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_46800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_46800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_50400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_50400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_54000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_54000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_57600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_57600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_61200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_61200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_64800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_64800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_68400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_68400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_72000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_72000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_7200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_7200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_75600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_75600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_79200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_79200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_82800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_82800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_SAMPLE_SIZE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_SAMPLE_SIZE.png" ),
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
        ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo, control );
        
        // Collect the file names actually written
        Set<Path> pathsWritten = control.get();
        Set<Path> actualFileNames = pathsWritten.stream()
                                          .map( Path::getFileName )
                                          .collect( Collectors.toSet() );

        // Expected file-name paths equals actual
        LOGGER.info( "Checking expected file names against actual file names for {} files...",
                     EXPECTED_FILE_NAMES.size() );
        assertEquals( EXPECTED_FILE_NAMES, actualFileNames );
        LOGGER.info( "Expected and actual file names match." );
        
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}

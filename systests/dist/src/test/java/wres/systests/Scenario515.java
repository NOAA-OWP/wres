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

public class Scenario515
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario514.class );
    private static final String NEWLINE = System.lineSeparator();

    /** Expected paths as file names. */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "pairs.csv.gz" ),
                    Path.of( "evaluation.csv.gz" ),
                    Path.of( "evaluation.csvt" ) );

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
                     + this.getClass()
                           .getSimpleName()
                           .toLowerCase()
                     + NEWLINE );
        this.scenarioInfo = new ScenarioInformation( this.getClass()
                                                         .getSimpleName()
                                                         .toLowerCase(),
                                                     ScenarioHelper.getBaseDirectory() );
        ScenarioHelper.logUsedSystemProperties( this.scenarioInfo );
    }

    @Test
    public void testScenario()
    {
        Set<Path> pathsWritten = ScenarioHelper.executeScenario( this.scenarioInfo );
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

        LOGGER.info( "########################################################## COMPLETED "
                     + this.getClass()
                           .getSimpleName()
                           .toLowerCase()
                     + NEWLINE );
    }
}
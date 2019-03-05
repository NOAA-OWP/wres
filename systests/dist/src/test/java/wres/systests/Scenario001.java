package wres.systests;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigPlus;
import wres.control.Control;
import wres.io.Operations;

public class Scenario001
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario001.class );
    private static final String NEWLINE = System.lineSeparator();

    private Scenario scenarioInfo;

    @Before
    public void beforeIndividualTest() throws IOException, SQLException
    {
        LOGGER.info( "{}{}",
                     "########################################################## EXECUTION ",
                     NEWLINE );
        Path baseDirectory = SystestsScenarioRunner.getBaseDirectory();
        this.scenarioInfo = new Scenario( this.getClass()
                                              .getSimpleName()
                                              .toLowerCase(),
                                          baseDirectory );
        Operations.cleanDatabase();
        SystestsScenarioRunner.deleteOldOutputDirectories( scenarioInfo.getScenarioDirectory() );
    }

    @Test
    public void testScenario()
    {
        Path config = this.scenarioInfo.getScenarioDirectory()
                                       .resolve( SystestsScenarioRunner.USUAL_EVALUATION_FILE_NAME );

        //Execute the control and return the exit code if its not zero.  No need to go further.
        String args[] = { config.toString() };
        Control wresEvaluation = new Control();
        int exitCode = wresEvaluation.apply( args );
        assertEquals( "Execution of WRES failed with exit code " + exitCode
                      + "; see log for more information!",
                      0,
                      exitCode );
        SystestsScenarioRunner.assertWRESOutputValid( wresEvaluation );
        SystestsScenarioRunner.assertOutputsMatchBenchmarks( this.scenarioInfo,
                                                             wresEvaluation );
    }
}

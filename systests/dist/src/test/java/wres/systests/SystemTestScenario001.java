package wres.systests;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.Operations;

public class SystemTestScenario001
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SystemTestScenario001.class );
    private static final String SCENARIO_NAME = "scenario001";
    private static final String NEWLINE = System.lineSeparator();

    @Before
    public void beforeEachIndividualTest() throws IOException, SQLException
    {
        LOGGER.info( "{}{}{}{}", NEWLINE, "########################################################## EXECUTION ", SCENARIO_NAME, NEWLINE );
        Operations.cleanDatabase();
        SystestsScenarioRunner.deleteOldOutputDirectories( Paths.get( SCENARIO_NAME ) );
    }

    @Test
    public void testScenario()
    {
        SystestsScenarioRunner classUnderTest = new SystestsScenarioRunner( SCENARIO_NAME );
        classUnderTest.assertProjectExecution();
        classUnderTest.assertOutputsMatchBenchmarks();
    }
}

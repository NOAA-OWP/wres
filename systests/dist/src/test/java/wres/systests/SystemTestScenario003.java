package wres.systests;

import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;

public class SystemTestScenario003
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SystemTestScenario003.class );
    private static final String SCENARIO_NAME = "scenario003";
    private static final String NEWLINE = System.lineSeparator();

    @Before
    public void beforeIndividualTest()
    {
        LOGGER.info( "{}{}{}{}", NEWLINE, "########################################################## EXECUTION ", SCENARIO_NAME, NEWLINE );
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

package wres.systests;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;

public class Scenario003
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario003.class );
    private static final String NEWLINE = System.lineSeparator();

    @Before
    public void beforeIndividualTest()
    {
        LOGGER.info( "{}{}",
                     "########################################################## EXECUTION ",
                     NEWLINE );
        Path baseDirectory = SystestsScenarioRunner.getBaseDirectory();
        SystestsScenarioRunner.deleteOldOutputDirectories(
                baseDirectory.resolve( this.getClass()
                                           .getSimpleName()
                                           .toLowerCase() ) );
    }

    @Test
    public void testScenario()
    {
        SystestsScenarioRunner classUnderTest =
                new SystestsScenarioRunner( this.getClass()
                                                .getSimpleName()
                                                .toLowerCase() );
        classUnderTest.assertProjectExecution();
        classUnderTest.assertOutputsMatchBenchmarks();
    }
}

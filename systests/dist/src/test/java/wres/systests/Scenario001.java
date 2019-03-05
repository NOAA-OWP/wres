package wres.systests;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.Operations;

public class Scenario001
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario001.class );
    private static final String NEWLINE = System.lineSeparator();

    @Before
    public void beforeIndividualTest() throws IOException, SQLException
    {
        LOGGER.info( "{}{}",
                     "########################################################## EXECUTION ",
                     NEWLINE );
        Path baseDirectory = SystestsScenarioRunner.getBaseDirectory();
        Operations.cleanDatabase();
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

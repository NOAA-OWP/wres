package wres.systests;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;

import wres.control.Control;

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
        Path baseDirectory = ScenarioHelper.getBaseDirectory();
        ScenarioHelper.deleteOldOutputDirectories(
                baseDirectory.resolve( this.getClass()
                                           .getSimpleName()
                                           .toLowerCase() ) );
    }

    @Test
    public void testScenario()
    {
        //ScenarioHelper.assertOutputsMatchBenchmarks(  new Control() );
    }
}

package wres.systests;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.Operations;
import wres.control.Control;
public class Scenario721
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario721.class );
    private static final String NEWLINE = System.lineSeparator();

    private Scenario scenarioInfo;

    @Before
    public void beforeIndividualTest() throws IOException, SQLException
    {
        LOGGER.info( "########################################################## EXECUTING "
                     + this.getClass().getSimpleName().toLowerCase()
                     + NEWLINE );
        Path baseDirectory = ScenarioHelper.getBaseDirectory();
        this.scenarioInfo = new Scenario( this.getClass()
                                              .getSimpleName()
                                              .toLowerCase(),
                                          baseDirectory );
        //LOGGER.info( "####>> Cleaning the database..." );
        //Operations.cleanDatabase();
        //ScenarioHelper.deleteOldOutputDirectories( scenarioInfo.getScenarioDirectory() );
        ScenarioHelper.logUsedSystemProperties( scenarioInfo );
    }

    @Test
    public void testScenario()
    {
        Control control = ScenarioHelper.assertExecuteScenario( scenarioInfo );
        
        //This method does it based on a file listing of the output directory.
        //The other choice can work if you have a Control available, in which case
        //you can get the output paths from the Control via its get method.
        ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo, control );
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}


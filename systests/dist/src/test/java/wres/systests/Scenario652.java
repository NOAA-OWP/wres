package wres.systests;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.Operations;

public class Scenario652
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario652.class );
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
        LOGGER.info( "####>> Cleaning the database..." );
        Operations.cleanDatabase();
        
        LOGGER.info( "####>> Removing any existing old output directories..." );
        ScenarioHelper.deleteOldOutputDirectories( scenarioInfo.getScenarioDirectory() );
        
        LOGGER.info( "####>> Setting properties for run based on user settings..." );
        ScenarioHelper.setAllPropertiesFromEnvVars( scenarioInfo );
    }

    @Test
    public void testScenario()
    {
        LOGGER.info( "####>> Beginning test execution..." );
        ScenarioHelper.assertExecuteScenario( scenarioInfo );
        
        //This method does it based on a file listing of the output directory.
        //The other choice can work if you have a Control available, in which case
        //you can get the output paths from the Control via its get method.
        LOGGER.info( "####>> Assert outputs match benchmarks..." );
        ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo );
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}


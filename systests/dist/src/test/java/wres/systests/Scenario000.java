package wres.systests;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import wres.control.Control;
public class Scenario000
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario000.class );
    private static final String NEWLINE = System.lineSeparator();

    private ScenarioInformation scenarioInfo;
	
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
		ScenarioHelper.assertExecuteDatabase( scenarioInfo );
        //Control control = ScenarioHelper.assertExecuteScenario( scenarioInfo );
        //ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo, control );
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}

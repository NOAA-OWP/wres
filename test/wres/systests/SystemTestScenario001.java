package wres.systests;

import org.junit.Test;

public class SystemTestScenario001
{
    @Test
    public void testScenario001()
    {
        String scenarioName = "scenario001";
        System.out.println( );
        System.out.println("########################################################## EXECUTION " + scenarioName);
        System.out.println( );
        
        SystestsScenarioRunner classUnderTest = new SystestsScenarioRunner( scenarioName );
        
        //TODO Not sure what to do about output at this point.
        System.setProperty( "java.io.tmpdir", System.getenv( "TESTS_DIR" ) + "/" + scenarioName );

        SystestsScenarioRunner.assertCleanDatabase();
        classUnderTest.assertDeletionOfOldOutputDirectories();
        classUnderTest.assertProjectExecution();
        classUnderTest.assertOutputsMatchBenchmarks();
    }
}

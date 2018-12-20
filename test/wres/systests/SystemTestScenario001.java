package wres.systests;

import org.junit.Test;

public class SystemTestScenario001
{
    @Test
    public void testScenario()
    {
        String scenarioName = "scenario001";
        System.out.println( );
        System.out.println("########################################################## EXECUTION " + scenarioName);
        System.out.println( );
        
        SystestsScenarioRunner classUnderTest = new SystestsScenarioRunner( scenarioName );

        SystestsScenarioRunner.assertCleanDatabase();
        classUnderTest.assertDeletionOfOldOutputDirectories();
        classUnderTest.assertProjectExecution();
        classUnderTest.assertOutputsMatchBenchmarks();
    }
}

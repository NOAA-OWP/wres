package wres.systests;

import org.junit.Test;

public class SystemTestScenario003
{
    @Test
    public void testScenario()
    {
        String scenarioName = "scenario003";
        System.out.println( );
        System.out.println("########################################################## EXECUTION " + scenarioName);
        System.out.println( );
        
        SystestsScenarioRunner classUnderTest = new SystestsScenarioRunner( scenarioName );
        classUnderTest.assertDeletionOfOldOutputDirectories();
        classUnderTest.assertProjectExecution();
        classUnderTest.assertOutputsMatchBenchmarks();
    }
}

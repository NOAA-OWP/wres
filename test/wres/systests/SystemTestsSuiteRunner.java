package wres.systests;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class SystemTestsSuiteRunner
{
    public static void main( String[] args )
    {
        Result result = JUnitCore.runClasses( SystemTestSuite.class );
        System.out.println();
        System.out.println( "=========================================================================================" );
        System.out.println( "Summary of failures:" );
        for ( Failure failure : result.getFailures() )
        {
            System.out.println( failure.toString() );
        }
        System.out.println( "=========================================================================================" );
        System.out.println( "Were all system tests execute successful? " + result.wasSuccessful() + " (false = no, true = yes) " );
        System.out.println();
        return;
    }
}

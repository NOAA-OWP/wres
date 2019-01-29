package wres.systests;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Main class for executing the suite of tests.
 * @author Hank.Herr
 *
 */
public class SystemTestsSuiteRunner
{
    public static void main( String[] args )
    {
        //TODO When I run this job, for some reason it never stops.  It just sits their idle after
        //the execution is complete.  I'm unsure why.
        
        Result result = JUnitCore.runClasses( SystemTestSuite.class );
        System.out.println();
        System.out.println( "=========================================================================================" );
        System.out.println( "Summary of failures:" );
        for ( Failure failure : result.getFailures() )
        {
            System.out.println( failure.toString() );
            System.out.println( "TRACE: " + failure.getTrace());
        }
        System.out.println( "=========================================================================================" );
        System.out.println( "Were all system tests execute successful? " + result.wasSuccessful() + " (false = no, true = yes) " );
        System.out.println();
        System.exit(0);
    }
}

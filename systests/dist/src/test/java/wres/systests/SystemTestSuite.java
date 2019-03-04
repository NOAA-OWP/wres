package wres.systests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith( Suite.class )

@Suite.SuiteClasses( {
                       SystemTestScenario001.class,
                       SystemTestScenario003.class
} )
/**
 * Specify the suite of JUnit tests above.  The suite is executed in order.
 * @author Hank.Herr
 *
 */
public class SystemTestSuite
{
}

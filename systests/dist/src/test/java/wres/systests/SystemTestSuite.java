package wres.systests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith( Suite.class )

@Suite.SuiteClasses( {
                       Scenario001.class,
                       Scenario003.class
} )
/**
 * Specify the suite of JUnit tests above.  The suite is executed in order.
 * @author Hank.Herr
 *
 */
public class SystemTestSuite
{
}

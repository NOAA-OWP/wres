package wres.systests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith( SystemTestsSuiteRunner.class )

@Suite.SuiteClasses( {
                       Scenario001.class,
                       Scenario003.class,
                       Scenario100.class,
                       Scenario200.class
} )

/**
 * Specify the suite of JUnit tests above.  The suite is executed in order.
 * @author Hank.Herr
 *
 */
public class SystemTestSuite
{
}

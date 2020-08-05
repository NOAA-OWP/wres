package wres.systests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.time.Instant;
import java.time.Duration;

import wres.io.Operations;
import wres.io.utilities.Database;
import wres.system.SystemSettings;

@RunWith( SystemTestsSuiteRunner.class )

@Suite.SuiteClasses( {
    Scenario001.class,
    Scenario003.class,
    Scenario007.class,
    Scenario008.class,
    Scenario009.class,
    Scenario010.class,
    Scenario012.class,
    Scenario015.class,
    Scenario016.class,
    Scenario017.class,
    Scenario018.class,
    Scenario019.class,
    Scenario050.class,
    Scenario051.class,
    Scenario052.class,
    Scenario053.class,
    Scenario1000.class,
    Scenario1001.class,
    Scenario100.class,
    Scenario101.class,
    Scenario102.class,
    Scenario103.class,
    Scenario104.class,
    Scenario105.class,
    Scenario106.class,
    Scenario107.class,
    Scenario200.class,
    Scenario300.class,
    Scenario301.class,
    Scenario302.class,
    Scenario303.class,
    Scenario304.class,
    Scenario305.class,
    Scenario400.class,
    Scenario401.class,
    Scenario402.class,
    Scenario403.class,
    Scenario404.class,
    Scenario405.class,
    Scenario407.class,
    Scenario408.class,
    Scenario409.class,
    Scenario500.class,
    Scenario501.class,
    Scenario502.class,
    Scenario504.class,
    Scenario505.class,
    Scenario506.class,
    Scenario507.class,
    Scenario508.class,
    Scenario509.class,
    Scenario600.class,
    Scenario601.class,
    Scenario650.class,
    Scenario651.class,
    Scenario652.class,
    Scenario653.class,
    Scenario700.class,
    Scenario701.class,
    Scenario702.class,
    Scenario703.class,
    Scenario704.class,
    Scenario705.class,
    Scenario706.class,
    Scenario720.class,
    Scenario721.class,
    Scenario750.class,
    Scenario801.class,
    Scenario802.class
} )

/**
 * Specify the suite of JUnit tests above.  The suite is executed in order.
 * @author Hank.Herr
 */
public class SystemTestSuite
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SystemTestSuite.class );

   /**
    * Cleans once before all tests.
    *
    * @throws SQLException if the test database could not be cleaned for any reason
    */
    @BeforeClass
    public static void runBeforeAllTests() throws SQLException
    {
        String dbName = System.getProperty( "wres.databaseName" );
        LOGGER.info( "Cleaning the test database instance {}...", dbName );
        SystemSettings systemSettings = SystemSettings.fromDefaultClasspathXmlFile();
        Database database = new Database( systemSettings );
        Instant started = Instant.now();
        Operations.cleanDatabase( database );
        Instant stopped = Instant.now();
        Duration duration = Duration.between( started, stopped );
        database.shutdown();

        LOGGER.info( "Finished cleaning the test database instance {}, which took {}.",
                     dbName,
                     duration );
    }
}

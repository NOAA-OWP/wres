package wres.io.retrieval.datashop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.DoubleUnaryOperator;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.database.Database;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

import wres.system.DatabaseConnectionSupplier;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link UnitMapper}.
 * @author james.brown@hydrosolved.com
 */

@RunWith( PowerMockRunner.class)
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class UnitMapperTest
{
    private static TestDatabase testDatabase;
    private static ComboPooledDataSource dataSource;
    private Connection rawConnection;
    private @Mock DatabaseConnectionSupplier mockConnectionSupplier;
    private Database liquibaseDatabase;

    @BeforeClass
    public static void oneTimeSetup()
    {
        // TODO: with HikariCP #54944, try to move this to @BeforeTest rather
        // than having a static one-time db. The only reason we have the static
        // variable instead of an instance variable is because c3p0 didn't work
        // properly with the instance variable.

        UnitMapperTest.testDatabase = new TestDatabase( "UnitMapperTest" );

        // Even when pool is closed/nulled/re-instantiated for each test, the
        // old c3p0 pool is somehow found by the 2nd and following test runs.
        // Got around it by having a single pool for all the tests.
        // Create our own test data source connecting to in-memory H2 database
        UnitMapperTest.dataSource = UnitMapperTest.testDatabase.getNewComboPooledDataSource();
    }

    @Before
    public void setup() throws Exception
    {
        // Also mock a plain datasource (which works per test unlike c3p0)
        this.rawConnection = DriverManager.getConnection( UnitMapperTest.testDatabase.getJdbcString() );
        Mockito.when( this.mockConnectionSupplier.get() ).thenReturn( this.rawConnection );

        // Set up a bare bones database with only the schema
        UnitMapperTest.testDatabase.createWresSchema( this.rawConnection );

        // Substitute raw connection where needed:
        PowerMockito.mockStatic( SystemSettings.class );
        PowerMockito.when( SystemSettings.class, "getRawDatabaseConnection" )
                    .thenReturn( this.rawConnection );

        PowerMockito.whenNew( DatabaseConnectionSupplier.class )
                    .withNoArguments()
                    .thenReturn( this.mockConnectionSupplier );

        // Substitute our H2 connection pool for both pools:
        PowerMockito.when( SystemSettings.class, "getConnectionPool" )
                    .thenReturn( UnitMapperTest.dataSource );
        PowerMockito.when( SystemSettings.class, "getHighPriorityConnectionPool" )
                    .thenReturn( UnitMapperTest.dataSource );

        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = UnitMapperTest.testDatabase.createNewLiquibaseDatabase( this.rawConnection );
        
        // Create the wres.MeasurementUnit table and the wres.UnitConversion table
        UnitMapperTest.testDatabase.createMeasurementUnitTable( this.liquibaseDatabase );
        UnitMapperTest.testDatabase.createUnitConversionTable( this.liquibaseDatabase );
    }

    @Test
    public void checkConversionOfCFSToCMS() throws SQLException
    {
        // Create the unit mapper for CMS
        UnitMapper mapper = UnitMapper.of( "CMS" );
        
        // Get the identifier for CFS
        String script = "SELECT measurementunit_id "
                        + "FROM wres.MeasurementUnit "
                        + "WHERE unit_name = 'CFS'";

        DataScripter scripter = new DataScripter( script );
        
        Integer measurementUnitId;
        
        try( DataProvider provider = scripter.buffer() )
        {
            measurementUnitId = provider.getInt( "measurementunit_id" );
        }
        
        DoubleUnaryOperator converter = mapper.getUnitMapper( measurementUnitId );
        
        // 1.0 CFS = 35.3147 CMS. Check with delta 5 d.p.
        assertEquals( 1.0, converter.applyAsDouble( 35.3147 ), 0.00001 );
        
        // Assertions completed, drop the tables
        UnitMapperTest.testDatabase.dropMeasurementUnitTable( this.rawConnection );
        UnitMapperTest.testDatabase.dropUnitConversionTable( this.rawConnection );  
    }
    
    @After
    public void tearDown() throws SQLException
    {
        UnitMapperTest.testDatabase.dropWresSchema( this.rawConnection );
        this.rawConnection.close();
        this.rawConnection = null;
    }

    @AfterClass
    public static void tearDownAfterAllTests()
    {
        UnitMapperTest.dataSource.close();
        UnitMapperTest.dataSource = null;
        UnitMapperTest.testDatabase = null;
    }
}

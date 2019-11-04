package wres.io.retrieval.datashop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;
import java.util.function.DoubleUnaryOperator;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;

import org.junit.After;
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
import wres.io.data.details.MeasurementDetails;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link UnitMapper}.
 * @author james.brown@hydrosolved.com
 */

@RunWith( PowerMockRunner.class )
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class UnitMapperTest
{
    private TestDatabase testDatabase;
    private ComboPooledDataSource dataSource;
    private Connection rawConnection;
    private @Mock DatabaseConnectionSupplier mockConnectionSupplier;

    @BeforeClass
    public static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void setup() throws Exception
    {
        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "SingleValuedForecastRetrieverTest" );
        this.dataSource = this.testDatabase.getNewComboPooledDataSource();

        // Create the connection and schema
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();
    }    

    @Test
    public void testConversionOfCFSToCMS() throws SQLException
    {
        // Create the unit mapper for CMS
        UnitMapper mapper = UnitMapper.of( "CMS" );

        // Obtain the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();
        String units = "CFS";
        measurement.setUnit( units );
        measurement.save();
        Integer measurementUnitId = measurement.getId();

        DoubleUnaryOperator converter = mapper.getUnitMapper( measurementUnitId );

        // 1.0 CFS = 35.3147 CMS. Check with delta 5 d.p.
        assertEquals( 1.0, converter.applyAsDouble( 35.3147 ), 0.00001 );
        
        // Test via unit name
        DoubleUnaryOperator namedConverter = mapper.getUnitMapper( units );
        assertEquals( 1.0, namedConverter.applyAsDouble( 35.3147 ), 0.00001 );              
    }

    @Test
    public void testIdentityConversionOfCMSToCMS() throws SQLException
    {
        // Create the unit mapper for CMS
        UnitMapper mapper = UnitMapper.of( "CMS" );

        // Obtain the measurement units for CMS
        MeasurementDetails measurement = new MeasurementDetails();
        String units = "CMS";
        measurement.setUnit( units );
        measurement.save();
        Integer measurementUnitId = measurement.getId();

        DoubleUnaryOperator converter = mapper.getUnitMapper( measurementUnitId );
        assertEquals( 1.0, converter.applyAsDouble( 1.0 ), 0.00001 );
        
        // Test via unit name
        DoubleUnaryOperator namedConverter = mapper.getUnitMapper( units );
        assertEquals( 1.0, namedConverter.applyAsDouble( 1.0 ), 0.00001 );              
    }    
    
    @After
    public void tearDown() throws SQLException
    {
        this.dropTheTablesAndSchema();
        this.rawConnection.close();
        this.rawConnection = null;
        this.testDatabase = null;
        this.dataSource = null;
    }

    /**
     * Does the basic set-up work to create a connection and schema.
     * 
     * @throws Exception if the set-up failed
     */

    private void createTheConnectionAndSchema() throws Exception
    {
        // Also mock a plain datasource (which works per test unlike c3p0)
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        Mockito.when( this.mockConnectionSupplier.get() ).thenReturn( this.rawConnection );

        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute raw connection where needed:
        PowerMockito.mockStatic( SystemSettings.class );
        PowerMockito.when( SystemSettings.class, "getRawDatabaseConnection" )
                    .thenReturn( this.rawConnection );

        PowerMockito.whenNew( DatabaseConnectionSupplier.class )
                    .withNoArguments()
                    .thenReturn( this.mockConnectionSupplier );

        // Substitute our H2 connection pool for both pools:
        PowerMockito.when( SystemSettings.class, "getConnectionPool" )
                    .thenReturn( this.dataSource );
        PowerMockito.when( SystemSettings.class, "getHighPriorityConnectionPool" )
                    .thenReturn( this.dataSource );
    }

    /**
     * Adds the required tables for the tests presented here, which is a subset of all tables.
     * @throws LiquibaseException if the tables could not be created
     */

    private void addTheDatabaseAndTables() throws LiquibaseException
    {
        // Create the required tables
        Database liquibaseDatabase =
                this.testDatabase.createNewLiquibaseDatabase( this.rawConnection );

        this.testDatabase.createMeasurementUnitTable( liquibaseDatabase );
        this.testDatabase.createUnitConversionTable( liquibaseDatabase );
    }   
    
    /**
     * Drops the schema, cascading to all tables.
     * @throws SQLException if any tables or the schema failed to drop
     */
    
    private void dropTheTablesAndSchema() throws SQLException
    {
        this.testDatabase.dropWresSchema( this.rawConnection );
        this.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }
}

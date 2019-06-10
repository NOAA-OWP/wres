package wres.io.data.details;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import wres.config.generated.ProjectConfig;
import wres.io.project.Project;
import wres.system.DatabaseConnectionSupplier;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

@RunWith( PowerMockRunner.class)
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class DetailsTest
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

        DetailsTest.testDatabase = new TestDatabase( "DetailsTest" );

        // Even when pool is closed/nulled/re-instantiated for each test, the
        // old c3p0 pool is somehow found by the 2nd and following test runs.
        // Got around it by having a single pool for all the tests.
        // Create our own test data source connecting to in-memory H2 database
        DetailsTest.dataSource = DetailsTest.testDatabase.getNewComboPooledDataSource();
    }

    @Before
    public void setup() throws Exception
    {
        // Also mock a plain datasource (which works per test unlike c3p0)
        this.rawConnection = DriverManager.getConnection( DetailsTest.testDatabase.getJdbcString() );
        Mockito.when( this.mockConnectionSupplier.get() ).thenReturn( this.rawConnection );

        // Set up a bare bones database with only the schema
        DetailsTest.testDatabase.createWresSchema( this.rawConnection );

        // Substitute raw connection where needed:
        PowerMockito.mockStatic( SystemSettings.class );
        PowerMockito.when( SystemSettings.class, "getRawDatabaseConnection" )
                    .thenReturn( this.rawConnection );

        PowerMockito.whenNew( DatabaseConnectionSupplier.class )
                    .withNoArguments()
                    .thenReturn( this.mockConnectionSupplier );

        // Substitute our H2 connection pool for both pools:
        PowerMockito.when( SystemSettings.class, "getConnectionPool" )
                    .thenReturn( DetailsTest.dataSource );
        PowerMockito.when( SystemSettings.class, "getHighPriorityConnectionPool" )
                    .thenReturn( DetailsTest.dataSource );

        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = DetailsTest.testDatabase.createNewLiquibaseDatabase( this.rawConnection );
    }

    @Test
    public void saveSourceDetails() throws SQLException, LiquibaseException
    {
        // Add the source table
        DetailsTest.testDatabase.createSourceTable( this.liquibaseDatabase );

        SourceDetails.SourceKey sourceKey = SourceDetails.createKey( URI.create( "/this/is/just/a/test" ),
                                                                     "2017-06-16 11:13:00",
                                                                     null,
                                                                     "abc123" );
        SourceDetails sourceDetails = new SourceDetails( sourceKey );
        sourceDetails.save();
        assertTrue( "Expected source details to have performed insert.",
                    sourceDetails.performedInsert() );
        assertNotNull( "Expected the id of the source to be non-null",
                       sourceDetails.getId() );

        // Remove the source table now that assertions have finished.
        DetailsTest.testDatabase.dropSourceTable( this.rawConnection );
        DetailsTest.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }

    @Test
    public void saveProjectDetails() throws SQLException, LiquibaseException
    {
        // Add the project table
        DetailsTest.testDatabase.createProjectTable( this.liquibaseDatabase );

        Project project = new Project( new ProjectConfig( null, null, null, null, null, null ),
                                                     321 );
        project.save();
        assertTrue( "Expected project details to have performed insert.",
                    project.performedInsert() );
        assertNotNull( "Expected the id of the source to be non-null",
                       project.getId() );

        // Remove the project table and liquibase tables
        DetailsTest.testDatabase.dropProjectTable( this.rawConnection );
        DetailsTest.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }

    @After
    public void tearDown() throws SQLException
    {
        DetailsTest.testDatabase.dropWresSchema( this.rawConnection );
        this.rawConnection.close();
        this.rawConnection = null;
    }

    @AfterClass
    public static void tearDownAfterAllTests()
    {
        DetailsTest.dataSource.close();
        DetailsTest.dataSource = null;
        DetailsTest.testDatabase = null;
    }
}

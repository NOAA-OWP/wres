package wres.io.data.details;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
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
import wres.io.utilities.DatabaseConnectionSupplier;
import wres.system.SystemSettings;

@RunWith( PowerMockRunner.class)
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class DetailsTest
{
    private static ComboPooledDataSource connectionPoolDataSource;
    private static String connectionString;
    private Connection rawConnection;
    private @Mock DatabaseConnectionSupplier mockConnectionSupplier;
    private Database liquibaseDatabase;

    @BeforeClass
    public static void oneTimeSetup() throws ClassNotFoundException
    {
        // TODO: with HikariCP #54944, try to move this to @BeforeTest rather
        // than having a static one-time db. The only reason we have the static
        // variable instead of an instance variable is because c3p0 didn't work
        // properly with the instance variable.

        Class.forName( "org.h2.Driver" );

        // Create our own test data source connecting to in-memory H2 database
        connectionPoolDataSource = new ComboPooledDataSource();
        //connectionPoolDataSource.resetPoolManager();

        //connectionPoolDataSource.setJdbcUrl( "jdbc:h2:mem:DetailsTest;DB_CLOSE_DELAY=-1" );

        // helps h2 use a subset of postgres' syntax or features:
        //connectionPoolDataSource.setJdbcUrl( "jdbc:h2:mem:DetailsTest;DB_CLOSE_DELAY=-1;MODE=PostgreSQL" );

        // Use this verbose one to figure out issues with queries/files/h2/etc:
        //connectionPoolDataSource.setJdbcUrl( "jdbc:h2:mem:DetailsTest;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;TRACE_LEVEL_SYSTEM_OUT=3" );
        //connectionPoolDataSource.setJdbcUrl( "jdbc:h2:mem:DetailsTest;MODE=PostgreSQL;TRACE_LEVEL_SYSTEM_OUT=3" );

        // Even when pool is closed/nulled/re-instantiated for each test, the
        // old c3p0 pool is somehow found by the 2nd and following test runs.
        // Got around it by having a single pool for all the tests.

        connectionString = "jdbc:h2:mem:DetailsTest;MODE=PostgreSQL;";
        connectionPoolDataSource.setJdbcUrl( connectionString );
        connectionPoolDataSource.setUnreturnedConnectionTimeout( 1 );
        connectionPoolDataSource.setDebugUnreturnedConnectionStackTraces( true );
    }

    @Before
    public void setup() throws Exception
    {
        // Also mock a plain datasource (which works per test unlike c3p0)
        this.rawConnection = DriverManager.getConnection( connectionString );
        Mockito.when( this.mockConnectionSupplier.get() ).thenReturn( this.rawConnection );

        // Set up a bare bones database with only the schema
        try ( Statement statement = this.rawConnection.createStatement() )
        {
            statement.execute( "CREATE SCHEMA wres" );
        }

        // Substitute raw connection where needed:
        PowerMockito.mockStatic( SystemSettings.class );
        PowerMockito.when( SystemSettings.class, "getRawDatabaseConnection" )
                    .thenReturn( this.rawConnection );

        PowerMockito.whenNew( DatabaseConnectionSupplier.class )
                    .withNoArguments()
                    .thenReturn( this.mockConnectionSupplier );

        // Substitute our H2 connection pool for both pools:
        PowerMockito.when( SystemSettings.class, "getConnectionPool" )
                    .thenReturn( DetailsTest.connectionPoolDataSource );
        PowerMockito.when( SystemSettings.class, "getHighPriorityConnectionPool" )
                    .thenReturn( DetailsTest.connectionPoolDataSource );

        // Set up a liquibase database to run migrations against.
        JdbcConnection liquibaseConnection = new JdbcConnection( this.rawConnection );
        this.liquibaseDatabase =
                DatabaseFactory.getInstance()
                               .findCorrectDatabaseImplementation( liquibaseConnection );
    }

    @Test
    public void saveSourceDetails() throws SQLException, LiquibaseException
    {
        // Add the source table
        Liquibase liquibase = new Liquibase( "database/wres.Source_v5.xml",
                                             new ClassLoaderResourceAccessor(),
                                             this.liquibaseDatabase );
        liquibase.update( new Contexts() );

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
        try ( Statement statement = this.rawConnection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Source" );
            statement.execute( "DROP TABLE public.databasechangelog; DROP TABLE public.databasechangeloglock;" );
        }
    }

    @Test
    public void saveProjectDetails() throws SQLException, LiquibaseException
    {
        // Add the project table
        Liquibase liquibase = new Liquibase( "database/wres.Project_v2.xml",
                                             new ClassLoaderResourceAccessor(),
                                             this.liquibaseDatabase );
        liquibase.update( new Contexts() );

        Project project = new Project( new ProjectConfig( null, null, null, null, null, null ),
                                                     321 );
        project.save();
        assertTrue( "Expected source details to have performed insert.",
                    project.performedInsert() );
        assertNotNull( "Expected the id of the source to be non-null",
                       project.getId() );

        // Remove the project table and liquibase tables
        try ( Statement statement = this.rawConnection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Project" );
            statement.execute( "DROP TABLE public.databasechangelog; DROP TABLE public.databasechangeloglock;" );
        }
    }

    @After
    public void tearDown() throws SQLException
    {
        try ( Statement statement = this.rawConnection.createStatement() )
        {
            statement.execute( "DROP SCHEMA wres CASCADE" );
        }

        this.rawConnection.close();
        this.rawConnection = null;
    }

    @AfterClass
    public static void tearDownAfterAllTests()
    {
        connectionPoolDataSource.close();
        connectionPoolDataSource = null;
    }
}

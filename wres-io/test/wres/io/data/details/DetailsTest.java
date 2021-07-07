package wres.io.data.details;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;

import com.zaxxer.hikari.HikariDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

public class DetailsTest
{
    private static final Random random = new Random( 5 );
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private @Mock SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;
    private Features featureCache;
    private @Mock Executor mockExecutor;
    private Connection rawConnection;
    private Database liquibaseDatabase;

    @Before
    public void setup() throws Exception
    {
        MockitoAnnotations.initMocks( this );
        this.testDatabase = new TestDatabase( "DetailsTest" + random.nextLong() );
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );

        Mockito.when( mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( "h2" );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = this.testDatabase.createNewLiquibaseDatabase( this.rawConnection );
        this.featureCache = new Features( this.wresDatabase );
    }

    @Test
    public void saveSourceDetails() throws SQLException, LiquibaseException
    {
        // Add the source table
        this.testDatabase.createSourceTable( this.liquibaseDatabase );

        SourceDetails.SourceKey sourceKey = SourceDetails.createKey( URI.create( "/this/is/just/a/test" ),
                                                                     "2017-06-16 11:13:00",
                                                                     null,
                                                                     "abc123" );
        SourceDetails sourceDetails = new SourceDetails( sourceKey );
        sourceDetails.save( this.wresDatabase );
        assertTrue( "Expected source details to have performed insert.",
                    sourceDetails.performedInsert() );
        assertNotNull( "Expected the id of the source to be non-null",
                       sourceDetails.getId() );

        // Remove the source table now that assertions have finished.
        this.testDatabase.dropSourceTable( this.rawConnection );
        this.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }

    @Test
    public void saveProjectDetails() throws SQLException, LiquibaseException
    {
        // Add the project table
        this.testDatabase.createProjectTable( this.liquibaseDatabase );

        Project project = new Project( this.mockSystemSettings,
                                       this.wresDatabase,
                                       this.featureCache,
                                       this.mockExecutor,
                                       new ProjectConfig( null, null, null, null, null, null ),
                                                     "321" );
        project.save();
        assertTrue( "Expected project details to have performed insert.",
                    project.performedInsert() );
        assertNotNull( "Expected the id of the source to be non-null",
                       project.getId() );

        // Remove the project table and liquibase tables
        this.testDatabase.dropProjectTable( this.rawConnection );
        this.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }

    @After
    public void tearDown() throws SQLException
    {
        this.testDatabase.dropWresSchema( this.rawConnection );
        this.rawConnection.close();
        this.rawConnection = null;
        this.dataSource.close();
        this.dataSource = null;
    }

}

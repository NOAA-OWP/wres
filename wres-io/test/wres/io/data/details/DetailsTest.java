package wres.io.data.details;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import org.junit.After;
import org.junit.Before;
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
    private TestDatabase testDatabase;
    private ComboPooledDataSource dataSource;
    private Connection rawConnection;
    private @Mock DatabaseConnectionSupplier mockConnectionSupplier;
    private Database liquibaseDatabase;

    @Before
    public void setup() throws Exception
    {
        
        // Previously, this used a test database and connection pool per class, 
        // rather than per test. This was due to issues with one or more layers, 
        // such as c3p0 or the H2 driver, but there were updates to these 
        // dependencies after the earlier observations. As of this changeset, it 
        // works. See #56214-92 ish for more
        this.testDatabase = new TestDatabase( "DetailsTest" );
        
        this.dataSource = this.testDatabase.getNewComboPooledDataSource();
        
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

        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = this.testDatabase.createNewLiquibaseDatabase( this.rawConnection );        
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
        sourceDetails.save();
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

        Project project = new Project( new ProjectConfig( null, null, null, null, null, null ),
                                                     321 );
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
    }

}

package wres.io.database.details;

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

import wres.io.database.ConnectionSupplier;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.Features;
import wres.io.database.TestDatabase;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

public class SourceDetailsTest
{
    private static final Random random = new Random( 5 );
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private @Mock SystemSettings mockSystemSettings;
    private @Mock DatabaseSettings mockDatabaseSettings;
    private @Mock ConnectionSupplier mockConnectionSupplier;
    private wres.io.database.Database wresDatabase;
    private @Mock DatabaseCaches mockCaches;
    private Connection rawConnection;
    private Database liquibaseDatabase;

    @Before
    public void setup() throws Exception
    {
        MockitoAnnotations.openMocks( this );
        this.testDatabase = new TestDatabase( "SourceDetailsTest" + random.nextLong() );
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );

        Mockito.when( this.mockSystemSettings.getDatabaseConfiguration() )
               .thenReturn( this.mockDatabaseSettings );
        Mockito.when( this.mockDatabaseSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockDatabaseSettings.getMaxPoolSize() )
               .thenReturn( 10 );
        Mockito.when( this.mockConnectionSupplier.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockConnectionSupplier.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockConnectionSupplier.getSystemSettings() )
               .thenReturn( this.mockSystemSettings );

        this.wresDatabase = new wres.io.database.Database( this.mockConnectionSupplier );
        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = this.testDatabase.createNewLiquibaseDatabase( this.rawConnection );
        Features featuresCache = new Features( this.wresDatabase );

        Mockito.when( this.mockCaches.getFeaturesCache() )
               .thenReturn( featuresCache );
    }

    @Test
    public void saveSourceDetails() throws SQLException, LiquibaseException
    {
        // Add the source table
        this.testDatabase.createSourceTable( this.liquibaseDatabase );

        SourceDetails sourceDetails = new SourceDetails( "abc123" );
        sourceDetails.setVariableName( "V" );
        sourceDetails.setMeasurementUnitId( 1L );
        sourceDetails.setFeatureId( 1L );
        sourceDetails.save( this.wresDatabase );
        assertTrue( "Expected source details to have performed insert.",
                    sourceDetails.performedInsert() );
        assertNotNull( "Expected the id of the source to be non-null",
                       sourceDetails.getId() );

        // Remove the source table now that assertions have finished.
        this.testDatabase.dropSourceTable( this.rawConnection );
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

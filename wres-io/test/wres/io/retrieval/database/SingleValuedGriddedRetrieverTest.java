package wres.io.retrieval.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static wres.statistics.generated.ReferenceTime.ReferenceTimeType.T0;
import static wres.io.retrieval.database.RetrieverTestConstants.*;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import com.zaxxer.hikari.HikariDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureKey;
import wres.io.concurrency.Executor;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.details.SourceDetails;
import wres.io.database.DataScripter;
import wres.io.database.TestDatabase;
import wres.io.project.DatabaseProject;
import wres.io.project.Project;
import wres.statistics.generated.TimeWindow;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedGriddedRetriever}.
 * @author James Brown
 */

public class SingleValuedGriddedRetrieverTest
{
    @Mock
    private SystemSettings mockSystemSettings;
    private wres.io.database.Database wresDatabase;
    @Mock
    private Executor mockExecutor;
    private TestDatabase testDatabase;
    private MeasurementUnits measurementUnitsCache;
    @Mock
    private DatabaseCaches mockCaches;
    private HikariDataSource dataSource;
    private Connection rawConnection;

    /**
     * A feature for testing.
     */
    private static final FeatureKey FEATURE = FeatureKey.of( MessageFactory.getGeometry( "POINT( 1 2 )",
                                                                                         null,
                                                                                         4326,
                                                                                         "POINT( 1 2 )" ) );

    /**
     * A variable name for testing.
     */

    private static final String VARIABLE_NAME = "QINE";

    /**
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.RIGHT;

    /**
     * The measurement units for testing.
     */

    private static final String UNITS = "m3/s";

    @BeforeClass
    public static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void setup() throws SQLException, LiquibaseException
    {
        MockitoAnnotations.openMocks( this );
        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create the connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute raw connection where needed:
        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockSystemSettings.getDatabaseMaximumPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.database.Database( this.mockSystemSettings );
        this.measurementUnitsCache = new MeasurementUnits( this.wresDatabase );
        Features featuresCache = new Features( this.wresDatabase );

        Mockito.when( this.mockCaches.getFeaturesCache() )
               .thenReturn( featuresCache );

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addFiveGriddedSourcesToTheDatabase();

    }

    @Test
    public void testGetFormsRequestForThreeOfFiveSources() throws Exception
    {
        // Set the time window filter, aka pool boundaries to select a subset of sources

        // Remove the last source by reference time
        Instant referenceStart = Instant.parse( "2017-06-16T14:12:00Z" );
        Instant referenceEnd = Instant.parse( "2017-06-17T02:12:00Z" ); // PT1M before the last reference time

        // Remove the first source by valid time       
        Instant validStart = Instant.parse( "2017-06-16T15:13:00Z" ); // The first valid time, which is exclusive
        Instant validEnd = Instant.parse( "2017-06-17T07:13:00Z" ); // The last valid time, which is inclusive

        // Retain all sources by lead duration
        Duration leadStart = Duration.ofHours( 0 );
        Duration leadEnd = Duration.ofHours( 5 );

        TimeWindow inner = MessageFactory.getTimeWindow( referenceStart,
                                                         referenceEnd,
                                                         validStart,
                                                         validEnd,
                                                         leadStart,
                                                         leadEnd );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Build the retriever
        SingleValuedGriddedRetriever retriever =
                (SingleValuedGriddedRetriever) new SingleValuedGriddedRetriever.Builder().setIsForecast( true )
                                                                                         .setFeatures( Set.of( FEATURE ) )
                                                                                         .setProjectId( PROJECT_ID )
                                                                                         .setLeftOrRightOrBaseline( SingleValuedGriddedRetrieverTest.LRB )
                                                                                         .setTimeWindow( timeWindow )
                                                                                         .setDatabase( this.wresDatabase )
                                                                                         .setMeasurementUnitsCache( this.measurementUnitsCache )
                                                                                         .setFeaturesCache( this.mockCaches.getFeaturesCache() )
                                                                                         .setVariableName( SingleValuedGriddedRetrieverTest.VARIABLE_NAME )
                                                                                         .build();

        List<String> actualPaths = retriever.getPaths();

        // Create the expected request
        List<String> expectedPaths = List.of( "/this/is/just/a/test/source_2.nc",
                                              "/this/is/just/a/test/source_3.nc",
                                              "/this/is/just/a/test/source_4.nc" );

        // Verify the captured request
        assertEquals( expectedPaths, actualPaths );
    }

    @After
    public void tearDown() throws SQLException
    {
        this.dropTheTablesAndSchema();
        this.rawConnection.close();
        this.rawConnection = null;
        this.testDatabase = null;
        this.dataSource.close();
        this.dataSource = null;
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
        this.testDatabase.createTimeScaleTable( liquibaseDatabase );
        this.testDatabase.createFeatureTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesReferenceTimeTable( liquibaseDatabase );
        this.testDatabase.createSourceTable( liquibaseDatabase );
        this.testDatabase.createProjectTable( liquibaseDatabase );
        this.testDatabase.createProjectSourceTable( liquibaseDatabase );
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

    /**
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     * 
     * @throws SQLException if the detailed set-up fails
     */

    private void addFiveGriddedSourcesToTheDatabase() throws SQLException
    {
        long measurementUnitId = this.measurementUnitsCache.getOrCreateMeasurementUnitId( UNITS );
        long featureId = this.mockCaches.getFeaturesCache()
                                        .getOrCreateFeatureId( FEATURE );
        // Add a project 
        Project project =
                new DatabaseProject( this.wresDatabase,
                                     this.mockCaches,
                                     null,
                                     new ProjectConfig( null, null, null, null, null, "test_gridded_project" ),
                                     PROJECT_HASH );
        boolean saved = project.save();

        assertTrue( saved );

        assertEquals( PROJECT_HASH, project.getHash() );

        // Add a source for each of five forecast lead durations and output times
        // Also, add a project source for each one
        Instant sequenceOrigin = Instant.parse( "2017-06-16T11:13:00Z" );
        String projectSourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ("
                                     + project.getId()
                                     + ",{0},''"
                                     + SingleValuedGriddedRetrieverTest.LRB.value()
                                     + "'')";


        for ( int i = 0; i < 5; i++ )
        {
            int nextLeadMinutes = ( i + 1 ) * 60;
            Instant nextTime = sequenceOrigin.plus( Duration.ofHours( 3 ).multipliedBy( ( i + 1 ) ) );

            SourceDetails sourceDetails = new SourceDetails( "abc12" + ( i + 3 ) );
            sourceDetails.setSourcePath( URI.create( "/this/is/just/a/test/source_" + ( i + 1 ) + ".nc" ) );
            sourceDetails.setLead( nextLeadMinutes );
            sourceDetails.setIsPointData( false );
            sourceDetails.setVariableName( VARIABLE_NAME );
            sourceDetails.setMeasurementUnitId( measurementUnitId );
            sourceDetails.setFeatureId( featureId );
            sourceDetails.save( this.wresDatabase );

            assertTrue( sourceDetails.performedInsert() );

            Long sourceId = sourceDetails.getId();
            assertNotNull( sourceId );

            // Insert the reference datetime
            String[] row = new String[3];
            row[0] = Long.toString( sourceId );

            // Reference time (instant)
            row[1] = nextTime.toString();

            // Reference time type
            row[2] = T0.toString();
            ArrayList<String[]> rows = new ArrayList<>( 1 );
            rows.add( row );

            List<String> columns = List.of( "source_id",
                                            "reference_time",
                                            "reference_time_type" );
            boolean[] quotedColumns = { false, true, true };
            this.wresDatabase.copy( "wres.TimeSeriesReferenceTime",
                                    columns,
                                    rows,
                                    quotedColumns );

            // Add a project source
            String insert = MessageFormat.format( projectSourceInsert, sourceId );

            DataScripter script = new DataScripter( this.wresDatabase, insert );
            int rowCount = script.execute();

            assertEquals( 1, rowCount );
        }
    }

}

package wres.io.retrieving.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static wres.statistics.generated.ReferenceTime.ReferenceTimeType.T0;
import static wres.io.retrieving.database.RetrieverTestHelper.*;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import com.zaxxer.hikari.HikariDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import wres.config.components.DataType;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.Source;
import wres.config.components.Variable;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.database.ConnectionSupplier;
import wres.io.database.DatabaseOperations;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.Features;
import wres.io.database.caching.MeasurementUnits;
import wres.io.database.details.SourceDetails;
import wres.io.database.TestDatabase;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.IngestResultNeedingRetry;
import wres.io.project.DatabaseProject;
import wres.io.project.Project;
import wres.reading.DataSource;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeWindow;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedGriddedRetriever}.
 * @author James Brown
 */

class SingleValuedGriddedRetrieverTest
{
    @Mock
    private SystemSettings mockSystemSettings;
    @Mock
    DatabaseSettings mockDatabaseSettings;
    private wres.io.database.Database wresDatabase;
    private TestDatabase testDatabase;
    private MeasurementUnits measurementUnitsCache;
    @Mock
    private ConnectionSupplier mockConnectionSupplier;
    @Mock
    private DatabaseCaches mockCaches;
    private HikariDataSource dataSource;
    private Connection rawConnection;
    private Long projectId;

    /** A feature for testing. */
    private static final Feature FEATURE = Feature.of( MessageUtilities.getGeometry( "POINT( 1 2 )",
                                                                                     null,
                                                                                     4326,
                                                                                     "POINT( 1 2 )" ) );

    /** A variable name for testing. */
    private static final String VARIABLE_NAME = "QINE";

    /** A {@link DatasetOrientation} for testing. */
    private static final DatasetOrientation ORIENTATION = DatasetOrientation.RIGHT;

    /** The measurement units for testing. */
    private static final String UNITS = "m3/s";

    /** Mocks to close. */
    private AutoCloseable mocks;

    @BeforeAll
    public static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @BeforeEach
    public void setup() throws SQLException, LiquibaseException
    {
        this.mocks = MockitoAnnotations.openMocks( this );
        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create the connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute raw connection where needed:
        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockConnectionSupplier.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockConnectionSupplier.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockConnectionSupplier.getSystemSettings() )
               .thenReturn( this.mockSystemSettings );
        Mockito.when( this.mockSystemSettings.getDatabaseConfiguration() )
               .thenReturn( mockDatabaseSettings );
        Mockito.when( this.mockDatabaseSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockDatabaseSettings.getMaxPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.database.Database( this.mockConnectionSupplier );
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
    void testGetFormsRequestForThreeOfFiveSources() throws Exception
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

        TimeWindow inner = MessageUtilities.getTimeWindow( referenceStart,
                                                           referenceEnd,
                                                           validStart,
                                                           validEnd,
                                                           leadStart,
                                                           leadEnd );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Build the retriever
        SingleValuedGriddedRetriever retriever =
                ( SingleValuedGriddedRetriever ) new SingleValuedGriddedRetriever.Builder()
                        .setIsForecast( true )
                        .setFeatures( Set.of( FEATURE ) )
                        .setProjectId( this.projectId )
                        .setDatasetOrientation( SingleValuedGriddedRetrieverTest.ORIENTATION )
                        .setTimeWindow( timeWindow )
                        .setDatabase( this.wresDatabase )
                        .setMeasurementUnitsCache( this.measurementUnitsCache )
                        .setFeaturesCache( this.mockCaches.getFeaturesCache() )
                        .setVariable( new Variable( VARIABLE_NAME, null, null ) )
                        .build();

        List<String> actualPaths = retriever.getPaths();

        // Create the expected request
        List<String> expectedPaths = List.of( "/this/is/just/a/test/source_2.nc",
                                              "/this/is/just/a/test/source_3.nc",
                                              "/this/is/just/a/test/source_4.nc" );

        // Verify the captured request
        assertEquals( expectedPaths, actualPaths );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        this.dropTheTablesAndSchema();
        this.rawConnection.close();
        this.rawConnection = null;
        this.testDatabase = null;
        this.dataSource.close();
        this.dataSource = null;
        this.mocks.close();
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

        this.testDatabase.createAllTables( liquibaseDatabase );
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

        // Add a source for each of five forecast lead durations and output times
        // Also, add a project source for each one
        Instant sequenceOrigin = Instant.parse( "2017-06-16T11:13:00Z" );
        List<IngestResult> ingestResults = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            int nextLeadMinutes = ( i + 1 ) * 60;
            Instant nextTime = sequenceOrigin.plus( Duration.ofHours( 3 )
                                                            .multipliedBy( ( i + 1 ) ) );

            SourceDetails sourceDetails = new SourceDetails( "abc12" + ( i + 3 ) );
            URI uri = URI.create( "/this/is/just/a/test/source_" + ( i + 1 ) + ".nc" );
            sourceDetails.setSourcePath( uri );
            sourceDetails.setLead( nextLeadMinutes );
            sourceDetails.setIsPointData( false );
            sourceDetails.setVariableName( VARIABLE_NAME );
            sourceDetails.setMeasurementUnitId( measurementUnitId );
            sourceDetails.setFeatureId( featureId );
            sourceDetails.save( this.wresDatabase );

            assertTrue( sourceDetails.performedInsert() );

            Long sourceId = sourceDetails.getId();
            assertNotNull( sourceId );

            DataSource source = DataSource.of( DataSource.DataDisposition.NETCDF_GRIDDED,
                                               Mockito.mock( Source.class ),
                                               DatasetBuilder.builder()
                                                             .build(),
                                               List.of(),
                                               uri,
                                               SingleValuedGriddedRetrieverTest.ORIENTATION,
                                               null );
            IngestResult ingestResult = new IngestResultNeedingRetry( source,
                                                                      DataType.SINGLE_VALUED_FORECASTS,
                                                                      sourceId );

            ingestResults.add( ingestResult );

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
            DatabaseOperations.insertIntoDatabase( this.wresDatabase,
                                                   "wres.TimeSeriesReferenceTime",
                                                   columns,
                                                   rows,
                                                   quotedColumns );
        }

        // Add a fake left source, which is needed for validation
        DataSource leftSource = DataSource.of( DataSource.DataDisposition.NETCDF_GRIDDED,
                                               Mockito.mock( Source.class ),
                                               DatasetBuilder.builder()
                                                             .build(),
                                               List.of(),
                                               URI.create( "http://foo" ),
                                               DatasetOrientation.LEFT,
                                               null );
        IngestResult leftResult = new IngestResultNeedingRetry( leftSource,
                                                                DataType.OBSERVATIONS,
                                                                ingestResults.get( 0 )
                                                                             .getSurrogateKey() );
        ingestResults.add( leftResult );

        // Add a project
        GriddedFeatures griddedFeatures = Mockito.mock( GriddedFeatures.class );
        Mockito.when( griddedFeatures.get() )
               .thenReturn( Set.of( FEATURE ) );

        Project project =
                new DatabaseProject( this.wresDatabase,
                                     this.mockCaches,
                                     griddedFeatures,
                                     EvaluationDeclarationBuilder.builder()
                                                                 .left( DatasetBuilder.builder()
                                                                                      .build() )
                                                                 .right( DatasetBuilder.builder()
                                                                                       .build() )
                                                                 .label( "test_gridded_project" )
                                                                 .build(),
                                     ingestResults );

        assertEquals( PROJECT_HASH, project.getHash() );

        this.projectId = project.getId();
    }

}

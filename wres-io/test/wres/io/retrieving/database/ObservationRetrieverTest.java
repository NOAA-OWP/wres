package wres.io.retrieving.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import static wres.statistics.generated.ReferenceTime.ReferenceTimeType.T0;
import static wres.io.retrieving.database.RetrieverTestHelper.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.TestData;
import wres.io.database.ConnectionSupplier;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.TestDatabase;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.project.Projects;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.io.retrieving.Retriever;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeWindow;
import wres.io.database.locking.DatabaseLockManager;
import wres.io.database.locking.DatabaseLockManagerNoop;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link ObservationRetriever}.
 * @author James Brown
 */

public class ObservationRetrieverTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ObservationRetrieverTest.class );
    private static final String SECOND_TIME = "2023-04-01T09:00:00Z";
    private static final String FIRST_TIME = "2023-04-01T03:00:00Z";

    @Mock
    private SystemSettings mockSystemSettings;
    @Mock
    DatabaseSettings mockDatabaseSettings;
    @Mock
    private ConnectionSupplier mockConnectionSupplier;
    private wres.io.database.Database wresDatabase;
    private DatabaseCaches caches;
    private DatabaseLockManager lockManager;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;
    private AutoCloseable mocks;

    /**
     * A {@link DatasetOrientation} for testing.
     */

    private static final DatasetOrientation ORIENTATION = DatasetOrientation.LEFT;

    /**
     * Error message when attempting to retrieve by identifier.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of observed time-series by identifier is not "
                                                      + "currently possible.";

    @BeforeClass
    public static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void setup() throws Exception
    {
        this.mocks = MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

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
        Mockito.when( this.mockSystemSettings.getMaximumThreadCount() )
               .thenReturn( 7 );
        Mockito.when( this.mockSystemSettings.getMaximumIngestThreads() )
               .thenReturn( 7 );

        this.wresDatabase = new wres.io.database.Database( this.mockConnectionSupplier );
        this.caches = DatabaseCaches.of( this.wresDatabase );
        this.lockManager = new DatabaseLockManagerNoop();

        // Create the connection and schema
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addAnObservedTimeSeriesWithTenEventsToTheDatabase();
    }

    @Test
    public void testRetrievalOfObservedTimeSeriesWithTenEvents()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> observedRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setFeaturesCache( this.caches.getFeaturesCache() )
                                                  .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeatures( Set.of( FEATURE ) )
                                                  .setDatasetOrientation( ORIENTATION )
                                                  .build();

        // Get the time-series
        Stream<TimeSeries<Double>> observedSeries = observedRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = observedSeries.toList();

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap(),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( FIRST_TIME ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 72.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 79.0 ) )
                       .addEvent( Event.of( Instant.parse( SECOND_TIME ), 86.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T10:00:00Z" ), 93.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testRetrievalOfPoolShapedObservedTimeSeriesWithSevenEvents()
    {
        // Build the pool boundaries
        TimeWindow inner = MessageFactory.getTimeWindow( Instant.parse( "2023-04-01T02:00:00Z" ),
                                                         Instant.parse( SECOND_TIME ) );
        TimeWindowOuter poolBoundaries = TimeWindowOuter.of( inner );

        // Build the retriever
        Retriever<TimeSeries<Double>> observedRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setFeaturesCache( this.caches.getFeaturesCache() )
                                                  .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeatures( Set.of( FEATURE ) )
                                                  .setTimeWindow( poolBoundaries )
                                                  .setDatasetOrientation( ORIENTATION )
                                                  .build();

        // Get the time-series
        Stream<TimeSeries<Double>> observedSeries = observedRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = observedSeries.toList();

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Assert correct number of events
        assertEquals( 7, actualSeries.getEvents().size() );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap(),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( FIRST_TIME ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 72.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 79.0 ) )
                       .addEvent( Event.of( Instant.parse( SECOND_TIME ), 86.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetAllIdentifiersThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setFeaturesCache( this.caches.getFeaturesCache() )
                                                  .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeatures( Set.of( FEATURE ) )
                                                  .setDatasetOrientation( ORIENTATION )
                                                  .build();

        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               forecastRetriever::getAllIdentifiers );

        assertEquals( NO_IDENTIFIER_ERROR, expected.getMessage() );
    }

    @Test
    public void testGetByIdentifierThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                  .setFeaturesCache( this.caches.getFeaturesCache() )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeatures( Set.of( FEATURE ) )
                                                  .setDatasetOrientation( ORIENTATION )
                                                  .build();

        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               () -> forecastRetriever.get( 123 ) );

        assertEquals( NO_IDENTIFIER_ERROR, expected.getMessage() );
    }

    @Test
    public void testGetByIdentifierStreamThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                  .setFeaturesCache( this.caches.getFeaturesCache() )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeatures( Set.of( FEATURE ) )
                                                  .setDatasetOrientation( ORIENTATION )
                                                  .build();

        LongStream longStream = LongStream.of();

        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               () -> forecastRetriever.get( longStream ) );

        assertEquals( NO_IDENTIFIER_ERROR, expected.getMessage() );
    }

    @After
    public void tearDown() throws Exception
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
     * Does the basic set-up work to create a connection and schema.
     * @throws SQLException if the set-up failed
     */

    private void createTheConnectionAndSchema() throws SQLException
    {
        // Also mock a plain datasource (which works per test unlike c3p0)
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );

        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );
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

    private void addAnObservedTimeSeriesWithTenEventsToTheDatabase() throws SQLException
    {
        DataSource leftData = TestData.generateDataSource( DatasetOrientation.LEFT,
                                                           DataType.OBSERVATIONS );
        DataSource rightData = TestData.generateDataSource( DatasetOrientation.RIGHT,
                                                            DataType.SINGLE_VALUED_FORECASTS );
        LOGGER.debug( "leftData: {}", leftData );
        LOGGER.debug( "rightData: {}", rightData );

        String featureName = FEATURE.getName();
        Geometry geometry = Geometry.newBuilder()
                                    .setName( featureName )
                                    .build();
        Set<GeometryTuple> features =
                Set.of( GeometryTuple.newBuilder()
                                     .setLeft( geometry )
                                     .setRight( geometry )
                                     .build() );

        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .features( new wres.config.yaml.components.Features( features ) )
                                            .build();

        TimeSeries<Double> timeSeriesOne = TestData.generateTimeSeriesDoubleWithNoReferenceTimes();

        DatabaseTimeSeriesIngester ingesterOne =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setIngestExecutor( RetrieverTestHelper.getIngestExecutor() )
                                                        .setCaches( this.caches )
                                                        .setLockManager( this.lockManager )
                                                        .build();
        Stream<TimeSeriesTuple> tupleStreamOne =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesOne, leftData ) );
        IngestResult ingestResultOne = ingesterOne.ingest( tupleStreamOne, leftData )
                                                  .get( 0 );

        TimeSeries<Double> timeSeriesTwo = TestData.generateTimeSeriesDoubleOne( T0 );

        DatabaseTimeSeriesIngester ingesterTwo =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setIngestExecutor( RetrieverTestHelper.getIngestExecutor() )
                                                        .setCaches( this.caches )
                                                        .setLockManager( this.lockManager )
                                                        .build();

        Stream<TimeSeriesTuple> tupleStreamTwo =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesTwo, rightData ) );
        IngestResult ingestResultTwo = ingesterTwo.ingest( tupleStreamTwo, rightData )
                                                  .get( 0 );
        List<IngestResult> results = List.of( ingestResultOne,
                                              ingestResultTwo );

        // Print the contents of the source table.
        try ( Statement statement = this.rawConnection.createStatement() )
        {
            ResultSet sourceData =
                    statement.executeQuery( "select source_id, hash, measurementunit_id, path from wres.source" );

            while ( sourceData.next() )
            {
                LOGGER.info( "source_id={} hash={} measurementunit_id={} path={}",
                             sourceData.getLong( "source_id" ),
                             sourceData.getString( "hash" ),
                             sourceData.getShort( "measurementunit_id" ),
                             sourceData.getString( "path" ) );
            }
        }

        LOGGER.info( "ingestResultOne: {}", ingestResultOne );
        LOGGER.info( "ingestResultTwo: {}", ingestResultTwo );
        Projects.getProject( this.wresDatabase,
                             declaration,
                             this.caches,
                             null,
                             results );
    }

}

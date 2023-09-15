package wres.io.retrieving.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static wres.statistics.generated.ReferenceTime.ReferenceTimeType.T0;
import static wres.io.retrieving.database.RetrieverTestConstants.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
import wres.io.database.ConnectionSupplier;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.TestDatabase;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.project.Project;
import wres.io.project.Projects;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
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

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link SingleValuedForecastRetriever}.
 * @author James Brown
 */

public class SingleValuedForecastRetrieverTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedForecastRetrieverTest.class );
    @Mock
    private SystemSettings mockSystemSettings;
    @Mock
    DatabaseSettings mockDatabaseSettings;
    @Mock
    ConnectionSupplier mockConnectionSupplier;
    private wres.io.database.Database wresDatabase;
    private DatabaseCaches caches;
    private DatabaseLockManager lockManager;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;

    @BeforeClass
    public static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void setup() throws SQLException, LiquibaseException, IOException
    {
        MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create the connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

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
        Mockito.when( this.mockSystemSettings.getMaximumThreadCount())
               .thenReturn( 7 );
        Mockito.when( this.mockSystemSettings.getMaximumIngestThreads() )
               .thenReturn( 7 );

        this.wresDatabase = new wres.io.database.Database( this.mockConnectionSupplier );
        this.caches = DatabaseCaches.of( this.wresDatabase );
        this.lockManager = new DatabaseLockManagerNoop();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase();
    }

    @Test
    public void testRetrievalOfTwoForecastTimeSeriesEachWithFiveEvents()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.caches.getFeaturesCache() )
                                                           .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setDatasetOrientation( DatasetOrientation.RIGHT )
                                                           .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = forecastRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.toList();

        // There are two time-series, so assert that
        assertEquals( 2, actualCollection.size() );
        TimeSeries<Double> actualSeriesOne = actualCollection.get( 0 );
        TimeSeries<Double> actualSeriesTwo = actualCollection.get( 1 );

        // Create the first expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builderOne = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeriesOne =
                builderOne.setMetadata( expectedMetadata )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata expectedMetadataTwo =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               T2023_04_01T17_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builderTwo = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeriesTwo =
                builderTwo.setMetadata( expectedMetadataTwo )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T18:00:00Z" ), 65.0 ) )
                          .addEvent( Event.of( T2023_04_01T19_00_00Z, 72.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T20:00:00Z" ), 79.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T21:00:00Z" ), 86.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T22:00:00Z" ), 93.0 ) )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );
    }

    @Test
    public void testRetrievalOfTwoForecastTimeSeriesWithinTimeWindow()
    {
        // Set the time window filter, aka pool boundaries
        Instant referenceStart = Instant.parse( "2023-03-31T23:00:00Z" );
        Instant validStart = Instant.parse( "2023-04-01T03:00:00Z" );
        Duration leadStart = Duration.ofHours( 1 );
        Duration leadEnd = Duration.ofHours( 4 );

        TimeWindow inner = MessageFactory.getTimeWindow( referenceStart,
                                                         T2023_04_01T19_00_00Z,
                                                         validStart,
                                                         T2023_04_01T19_00_00Z,
                                                         leadStart,
                                                         leadEnd );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.caches.getFeaturesCache() )
                                                           .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setTimeWindow( timeWindow )
                                                           .setDatasetOrientation( DatasetOrientation.RIGHT )
                                                           .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = forecastRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.toList();

        // There are two time-series, so assert that
        assertEquals( 2, actualCollection.size() );
        TimeSeries<Double> actualSeriesOne = actualCollection.get( 0 );
        TimeSeries<Double> actualSeriesTwo = actualCollection.get( 1 );

        // Create the first expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builderOne = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeriesOne =
                builderOne.setMetadata( expectedMetadata )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata expectedMetadataTwo =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               T2023_04_01T17_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builderTwo = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeriesTwo =
                builderTwo.setMetadata( expectedMetadataTwo )
                          .addEvent( Event.of( T2023_04_01T19_00_00Z, 72.0 ) )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );
    }

    @Test
    public void testGetRetrievalOfTimeSeriesIdentifiersReturnsTwoIdentifiers()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.caches.getFeaturesCache() )
                                                           .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setDatasetOrientation( DatasetOrientation.RIGHT )
                                                           .build();

        // Get the time-series
        List<Long> identifiers = forecastRetriever.getAllIdentifiers()
                                                  .boxed()
                                                  .toList();

        // Actual number of time-series equals expected number
        assertEquals( 2, identifiers.size() );
    }

    @Test
    public void testGetRetrievalOfTimeSeriesByIdentifierReturnsTwoTimeSeries()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.caches.getFeaturesCache() )
                                                           .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setDatasetOrientation( DatasetOrientation.RIGHT )
                                                           .build();

        // Get the time-series
        LongStream identifiers = forecastRetriever.getAllIdentifiers();

        // Actual number of time-series equals expected number
        assertEquals( 2, forecastRetriever.get( identifiers ).count() );
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
     * Performs the detailed set-up work to add two time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     *
     * @throws SQLException if the detailed set-up fails
     */

    private void addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase() throws SQLException, IOException
    {
        DataSource leftData = RetrieverTestData.generateDataSource( DatasetOrientation.LEFT,
                                                                    DataType.OBSERVATIONS );
        DataSource rightData = RetrieverTestData.generateDataSource( DatasetOrientation.RIGHT,
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

        TimeSeries<Double> timeSeriesOne = RetrieverTestData.generateTimeSeriesDoubleOne( T0 );
        IngestResult ingestResultOne;
        try ( DatabaseTimeSeriesIngester ingesterOne =
                      new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                              .setDatabase( this.wresDatabase )
                                                              .setCaches( this.caches )
                                                              .setLockManager( this.lockManager )
                                                              .build() )
        {
            Stream<TimeSeriesTuple> tupleStreamOne =
                    Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesOne, rightData ) );
            ingestResultOne = ingesterOne.ingest( tupleStreamOne, rightData )
                                         .get( 0 );
        }

        TimeSeries<Double> timeSeriesTwo = RetrieverTestData.generateTimeSeriesDoubleFour();
        IngestResult ingestResultTwo;
        try ( DatabaseTimeSeriesIngester ingesterTwo =
                      new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                              .setDatabase( this.wresDatabase )
                                                              .setCaches( this.caches )
                                                              .setLockManager( this.lockManager )
                                                              .build() )
        {
            Stream<TimeSeriesTuple> tupleStreamTwo =
                    Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesTwo, rightData ) );
            ingestResultTwo = ingesterTwo.ingest( tupleStreamTwo, rightData )
                                         .get( 0 );
        }

        TimeSeries<Double> timeSeriesThree = RetrieverTestData.generateTimeSeriesDoubleWithNoReferenceTimes();
        IngestResult ingestResultThree;
        try ( DatabaseTimeSeriesIngester ingesterThree =
                      new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                              .setDatabase( this.wresDatabase )
                                                              .setCaches( this.caches )
                                                              .setLockManager( this.lockManager )
                                                              .build() )
        {
            Stream<TimeSeriesTuple> tupleStreamThree =
                    Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesThree, leftData ) );
            ingestResultThree = ingesterThree.ingest( tupleStreamThree, leftData )
                                             .get( 0 );
        }

        List<IngestResult> results = List.of( ingestResultOne,
                                              ingestResultTwo,
                                              ingestResultThree );

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
        LOGGER.info( "ingestResultThree: {}", ingestResultThree );
        Project project = Projects.getProject( this.wresDatabase,
                                               declaration,
                                               this.caches,
                                               null,
                                               results );
        assertTrue( project.save() );
    }

}

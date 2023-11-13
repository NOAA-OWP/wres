package wres.io.retrieving.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
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

import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureTuple;
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
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeWindow;
import wres.io.database.locking.DatabaseLockManager;
import wres.io.database.locking.DatabaseLockManagerNoop;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link EnsembleRetrieverFactory}.
 * @author James Brown
 */

public class EnsembleRetrieverFactoryTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleRetrieverFactoryTest.class );
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
    private ExecutorService ingestExecutor;
    private AutoCloseable mocks;

    /**
     * The retriever factory to test.
     */

    private EnsembleRetrieverFactory factoryToTest;

    @BeforeClass
    public static void runOnceBeforeAllTests()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void runBeforeEachTest() throws SQLException, LiquibaseException
    {
        this.mocks = MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create an ingest executor
        this.ingestExecutor = RetrieverTestHelper.getIngestExecutor();

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

        // Create the connection and schema, set up mock settings
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();
        this.caches = DatabaseCaches.of( this.wresDatabase );
        this.lockManager = new DatabaseLockManagerNoop();

        // Add some data for testing
        this.addTimeSeriesToDatabase();

        // Create the retriever factory to test
        this.createEnsembleRetrieverFactory();
    }

    @Test
    public void testGetLeftRetrieverReturnsOneTimeSeriesWithTenEvents()
    {
        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( Set.of( FEATURE ) )
                                                                      .get()
                                                                      .toList();

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
                       .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 72.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 79.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T09:00:00Z" ), 86.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T10:00:00Z" ), 93.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetLeftRetrieverWithTimeWindowReturnsOneTimeSeriesWithFiveEvents()
    {
        // The time window to select events
        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.parse( "2023-04-01T02:00:00Z" ),
                                                                         T2023_04_01T07_00_00Z );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( Set.of( FEATURE ),
                                                                                         timeWindow )
                                                                      .get()
                                                                      .toList();

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
                       .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 72.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetRightRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                                         Instant.parse( "2023-04-01T00:00:00Z" ),
                                                                         Instant.parse( "2023-04-01T01:00:00Z" ),
                                                                         Instant.parse( "2023-04-01T04:00:00Z" ) );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Ensemble>> actualCollection = this.factoryToTest.getRightRetriever( Set.of( FEATURE ),
                                                                                            timeWindow )
                                                                        .get()
                                                                        .toList();

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Ensemble> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( T0,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<>();

        Labels expectedLabels = Labels.of( "123", "456", "567" );

        TimeSeries<Ensemble> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ),
                                            Ensemble.of( new double[] { 37.0, 107.0, 72.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( T2023_04_01T03_00_00Z,
                                            Ensemble.of( new double[] { 44.0, 114.0, 79.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( T2023_04_01T04_00_00Z,
                                            Ensemble.of( new double[] { 51.0, 121.0, 86.0 }, expectedLabels ) ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetBaselineRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                                         T2023_04_01T00_00_00Z,
                                                                         T2023_04_01T01_00_00Z,
                                                                         T2023_04_01T04_00_00Z );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Ensemble>> actualCollection = this.factoryToTest.getBaselineRetriever( Set.of( FEATURE ),
                                                                                               timeWindow )
                                                                        .get()
                                                                        .toList();

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Ensemble> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( T0,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<>();

        Labels expectedLabels = Labels.of( "123", "456", "567" );

        TimeSeries<Ensemble> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ),
                                            Ensemble.of( new double[] { 37.0, 107.0, 72.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( T2023_04_01T03_00_00Z,
                                            Ensemble.of( new double[] { 44.0, 114.0, 79.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( T2023_04_01T04_00_00Z,
                                            Ensemble.of( new double[] { 51.0, 121.0, 86.0 }, expectedLabels ) ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @After
    public void runAfterEachTest() throws Exception
    {
        this.dropTheTablesAndSchema();
        this.rawConnection.close();
        this.rawConnection = null;
        this.testDatabase = null;
        this.dataSource.close();
        this.dataSource = null;
        this.ingestExecutor.shutdownNow();
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
     * Creates an instance of a {@link EnsembleRetrieverFactory} to test.
     */

    private void createEnsembleRetrieverFactory()
    {
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder()
                                                               .name( VARIABLE_NAME )
                                                               .build() )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( VARIABLE_NAME )
                                                                .build() )
                                      .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( right )
                                                         .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .baseline( baseline )
                                            .unit( UNIT )
                                            .build();

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, FEATURE );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );

        Set<FeatureTuple> allFeatures = Set.of( featureTuple );
        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );
        Mockito.when( project.getId() )
               .thenReturn( PROJECT_ID );
        Mockito.when( project.getFeatures() )
               .thenReturn( allFeatures );
        Mockito.when( project.getVariableName( Mockito.any() ) )
               .thenReturn( VARIABLE_NAME );
        Mockito.when( project.getVariableName( Mockito.any() ) )
               .thenReturn( VARIABLE_NAME );
        Mockito.when( project.getDeclaredDataset( DatasetOrientation.LEFT ) )
               .thenReturn( left );
        Mockito.when( project.getDeclaredDataset( DatasetOrientation.RIGHT ) )
               .thenReturn( right );
        Mockito.when( project.getDeclaredDataset( DatasetOrientation.BASELINE ) )
               .thenReturn( right ); // Same as right
        Mockito.when( project.hasBaseline() )
               .thenReturn( true );
        Mockito.when( project.hasProbabilityThresholds() )
               .thenReturn( false );

        // Create the factory instance
        this.factoryToTest = EnsembleRetrieverFactory.of( project, this.wresDatabase, this.caches );
    }

    /**
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     *
     * @throws SQLException if the detailed set-up fails
     */

    private void addTimeSeriesToDatabase() throws SQLException
    {
        DataSource leftData = RetrieverTestData.generateDataSource( DatasetOrientation.LEFT,
                                                                    DataType.OBSERVATIONS );
        DataSource rightData = RetrieverTestData.generateDataSource( DatasetOrientation.RIGHT,
                                                                     DataType.ENSEMBLE_FORECASTS );
        DataSource baselineData = RetrieverTestData.generateBaselineDataSource( DataType.ENSEMBLE_FORECASTS );
        LOGGER.debug( "leftData: {}", leftData );
        LOGGER.debug( "rightData: {}", rightData );
        LOGGER.debug( "baselineData: {}", baselineData );

        String featureName = FEATURE.getName();
        Geometry geometry = Geometry.newBuilder()
                                    .setName( featureName )
                                    .build();
        Set<GeometryTuple> features =
                Set.of( GeometryTuple.newBuilder()
                                     .setLeft( geometry )
                                     .setRight( geometry )
                                     .setBaseline( geometry )
                                     .build() );

        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();
        Dataset baselineDatset = DatasetBuilder.builder()
                                               .type( DataType.ENSEMBLE_FORECASTS )
                                               .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDatset )
                                                         .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .baseline( baseline )
                                            .features( new wres.config.yaml.components.Features( features ) )
                                            .build();

        TimeSeries<Ensemble> timeSeriesOne = RetrieverTestData.generateTimeSeriesEnsembleOne();
        DatabaseTimeSeriesIngester ingesterOne =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setIngestExecutor( this.ingestExecutor )
                                                        .setLockManager( this.lockManager )
                                                        .build();

        Stream<TimeSeriesTuple> tupleStreamOne =
                Stream.of( TimeSeriesTuple.ofEnsemble( timeSeriesOne, rightData ) );
        IngestResult ingestResultOne = ingesterOne.ingest( tupleStreamOne, rightData )
                                                  .get( 0 );

        DatabaseTimeSeriesIngester ingesterTwo =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setIngestExecutor( this.ingestExecutor )
                                                        .setLockManager( this.lockManager )
                                                        .build();

        Stream<TimeSeriesTuple> tupleStreamTwo =
                Stream.of( TimeSeriesTuple.ofEnsemble( timeSeriesOne, baselineData ) );
        IngestResult ingestResultTwo = ingesterTwo.ingest( tupleStreamTwo, baselineData )
                                                  .get( 0 );
        TimeSeries<Double> timeSeriesTwo = RetrieverTestData.generateTimeSeriesDoubleWithNoReferenceTimes();

        DatabaseTimeSeriesIngester ingesterThree =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setIngestExecutor( this.ingestExecutor )
                                                        .setLockManager( this.lockManager )
                                                        .build();

        Stream<TimeSeriesTuple> tupleStreamThree =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesTwo, leftData ) );
        IngestResult ingestResultThree = ingesterThree.ingest( tupleStreamThree, leftData )
                                                      .get( 0 );
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

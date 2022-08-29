package wres.io.retrieval.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static wres.datamodel.time.ReferenceTimeType.T0;
import static wres.io.retrieval.database.RetrieverTestConstants.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
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

import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.data.caching.DatabaseCaches;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.project.Project;
import wres.io.project.Projects;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.retrieval.UnitMapper;
import wres.io.utilities.TestDatabase;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeWindow;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedRetrieverFactory}.
 * @author James Brown
 */

public class SingleValuedRetrieverFactoryTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedRetrieverFactoryTest.class );
    @Mock
    private SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;
    @Mock
    private Executor mockExecutor;
    @Mock
    private ProjectConfig mockProjectConfig;
    private DatabaseCaches caches;
    private DatabaseLockManager lockManager;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;

    /**
     * The retriever factory to test.
     */

    private SingleValuedRetrieverFactory factoryToTest;

    @BeforeClass
    public static void runOnceBeforeAllTests()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void runBeforeEachTest() throws SQLException, LiquibaseException
    {
        MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create the connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockSystemSettings.getMaximumPoolSize() )
               .thenReturn( 10 );
        Mockito.when( this.mockSystemSettings.maximumThreadCount() )
               .thenReturn( 7 );
        PairConfig pairConfig = Mockito.mock( PairConfig.class );
        Mockito.when( pairConfig.getGridSelection() )
               .thenReturn( List.of() );
        Mockito.when( this.mockProjectConfig.getPair() )
               .thenReturn( pairConfig );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        this.caches = DatabaseCaches.of( this.wresDatabase, this.mockProjectConfig );
        this.lockManager = new DatabaseLockManagerNoop();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase();

        // Create the retriever factory to test
        this.createSingleValuedRetrieverFactory();
    }

    @Test
    public void testGetLeftRetrieverReturnsOneTimeSeriesWithTenEvents()
    {

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( Set.of( FEATURE ) )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

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
                       .addEvent( Event.of( T2023_04_01T01_00_00Z, 30.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                       .addEvent( Event.of( T2023_04_01T03_00_00Z, 44.0 ) )
                       .addEvent( Event.of( T2023_04_01T04_00_00Z, 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( T2023_04_01T07_00_00Z, 72.0 ) )
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
        TimeWindow inner = MessageFactory.getTimeWindow( Instant.parse( "2023-04-01T02:00:00Z" ),
                                                         T2023_04_01T07_00_00Z );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( Set.of( FEATURE ),
                                                                                         timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

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
                       .addEvent( Event.of( T2023_04_01T03_00_00Z, 44.0 ) )
                       .addEvent( Event.of( T2023_04_01T04_00_00Z, 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( T2023_04_01T07_00_00Z, 72.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetRightRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindow inner = MessageFactory.getTimeWindow( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                         T2023_04_01T00_00_00Z,
                                                         T2023_04_01T01_00_00Z,
                                                         T2023_04_01T04_00_00Z );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getRightRetriever( Set.of( FEATURE ),
                                                                                          timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( T0,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                       .addEvent( Event.of( T2023_04_01T03_00_00Z, 44.0 ) )
                       .addEvent( Event.of( T2023_04_01T04_00_00Z, 51.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetBaselineRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindow inner = MessageFactory.getTimeWindow( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                         T2023_04_01T00_00_00Z,
                                                         T2023_04_01T01_00_00Z,
                                                         T2023_04_01T04_00_00Z );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getBaselineRetriever( Set.of( FEATURE ),
                                                                                             timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( T0,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                       .addEvent( Event.of( T2023_04_01T03_00_00Z, 44.0 ) )
                       .addEvent( Event.of( T2023_04_01T04_00_00Z, 51.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @After
    public void runAfterEachTest() throws SQLException
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
     * Creates an instance of a {@link SingleValuedRetrieverFactory} to test.
     */

    private void createSingleValuedRetrieverFactory()
    {
        // Mock the sufficient elements of the ProjectConfig
        PairConfig pairsConfig = new PairConfig( UNIT,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );
        List<DataSourceConfig.Source> sourceList = new ArrayList<>();

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      sourceList,
                                                      new Variable( VARIABLE_NAME, null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        // Same right and baseline
        DataSourceBaselineConfig rightAndBaseline =
                new DataSourceBaselineConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                              sourceList,
                                              new Variable( VARIABLE_NAME, null ),
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              false );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      rightAndBaseline,
                                                                      rightAndBaseline );

        ProjectConfig projectConfig = new ProjectConfig( inputsConfig, pairsConfig, null, null, null, null );

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, null );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );

        Set<FeatureTuple> allFeatures = Set.of( featureTuple );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( PROJECT_ID );
        Mockito.when( project.getFeatures() ).thenReturn( allFeatures );
        Mockito.when( project.getVariableName( Mockito.any( LeftOrRightOrBaseline.class ) ) )
               .thenReturn( VARIABLE_NAME );
        Mockito.when( project.hasBaseline() ).thenReturn( true );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );

        // Create the factory instance
        UnitMapper unitMapper = UnitMapper.of( UNIT );
        this.factoryToTest = SingleValuedRetrieverFactory.of( project, this.wresDatabase, this.caches, unitMapper );
    }

    /**
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     * 
     * @throws SQLException if the detailed set-up fails
     */

    private void addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase() throws SQLException
    {
        DataSource leftData = RetrieverTestData.generateDataSource( LeftOrRightOrBaseline.LEFT,
                                                                    DatasourceType.OBSERVATIONS );
        DataSource rightData = RetrieverTestData.generateDataSource( LeftOrRightOrBaseline.RIGHT,
                                                                     DatasourceType.SINGLE_VALUED_FORECASTS );
        LOGGER.info( "leftData: {}", leftData );
        LOGGER.info( "rightData: {}", rightData );
        ProjectConfig.Inputs fakeInputs =
                new ProjectConfig.Inputs( leftData.getContext(), rightData.getContext(), null );
        ProjectConfig fakeConfig = new ProjectConfig( fakeInputs, null, null, null, null, null );
        TimeSeries<Double> timeSeriesOne = RetrieverTestData.generateTimeSeriesDoubleOne( T0 );
        TimeSeriesIngester ingesterOne =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setProjectConfig( fakeConfig )
                                                        .setLockManager( this.lockManager )
                                                        .build();
        Stream<TimeSeriesTuple> tupleStreamOne =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesOne, rightData ) );
        IngestResult ingestResultOne = ingesterOne.ingest( tupleStreamOne, rightData )
                                                  .get( 0 );
        TimeSeries<Double> timeSeriesTwo = RetrieverTestData.generateTimeSeriesDoubleFour( T0 );

        TimeSeriesIngester ingesterTwo =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setProjectConfig( fakeConfig )
                                                        .setLockManager( this.lockManager )
                                                        .build();
        Stream<TimeSeriesTuple> tupleStreamTwo =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesTwo, rightData ) );
        IngestResult ingestResultTwo = ingesterTwo.ingest( tupleStreamTwo, rightData )
                                                  .get( 0 );

        TimeSeries<Double> timeSeriesThree = RetrieverTestData.generateTimeSeriesDoubleWithNoReferenceTimes();

        TimeSeriesIngester ingesterThree =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setProjectConfig( fakeConfig )
                                                        .setLockManager( this.lockManager )
                                                        .build();
        Stream<TimeSeriesTuple> tupleStreamThree =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesThree, leftData ) );
        IngestResult ingestResultThree =
                ingesterThree.ingest( tupleStreamThree, leftData )
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
        Project project = Projects.getProjectFromIngest( this.wresDatabase,
                                                         this.caches,
                                                         fakeConfig,
                                                         results );
        assertTrue( project.save() );
    }
}

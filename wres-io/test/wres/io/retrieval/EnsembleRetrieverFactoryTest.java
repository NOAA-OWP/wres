package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static wres.datamodel.time.ReferenceTimeType.T0;
import static wres.io.retrieval.RetrieverTestConstants.*;

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
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.TimeScales;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.project.Project;
import wres.io.project.Projects;
import wres.io.reading.DataSource;
import wres.io.utilities.TestDatabase;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeWindow;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
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
    private wres.io.utilities.Database wresDatabase;
    @Mock
    private Executor mockExecutor;
    private Features featuresCache;
    private MeasurementUnits measurementUnitsCache;
    private TimeScales timeScalesCache;
    private Ensembles ensemblesCache;
    private DatabaseLockManager lockManager;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;

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
        MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockSystemSettings.getMaximumPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );

        // Create the connection and schema, set up mock settings
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();

        this.featuresCache = new Features( this.wresDatabase );
        this.measurementUnitsCache = new MeasurementUnits( this.wresDatabase );
        this.timeScalesCache = new TimeScales( this.wresDatabase );
        this.ensemblesCache = new Ensembles( this.wresDatabase );
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
        TimeWindow inner = MessageFactory.getTimeWindow( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                         Instant.parse( "2023-04-01T00:00:00Z" ),
                                                         Instant.parse( "2023-04-01T01:00:00Z" ),
                                                         Instant.parse( "2023-04-01T04:00:00Z" ) );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Ensemble>> actualCollection = this.factoryToTest.getRightRetriever( Set.of( FEATURE ),
                                                                                            timeWindow )
                                                                        .get()
                                                                        .collect( Collectors.toList() );

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
        TimeWindow inner = MessageFactory.getTimeWindow( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                         T2023_04_01T00_00_00Z,
                                                         T2023_04_01T01_00_00Z,
                                                         T2023_04_01T04_00_00Z );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Get the actual left series
        List<TimeSeries<Ensemble>> actualCollection = this.factoryToTest.getBaselineRetriever( Set.of( FEATURE ),
                                                                                               timeWindow )
                                                                        .get()
                                                                        .collect( Collectors.toList() );

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
                new DataSourceBaselineConfig( DatasourceType.fromValue( "ensemble forecasts" ),
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

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, FEATURE );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );

        Set<FeatureTuple> allFeatures = Set.of( featureTuple );
        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( PROJECT_ID );
        Mockito.when( project.getFeatures() ).thenReturn( allFeatures );
        Mockito.when( project.getLeftVariableName() ).thenReturn( VARIABLE_NAME );
        Mockito.when( project.getRightVariableName() ).thenReturn( VARIABLE_NAME );
        Mockito.when( project.getBaselineVariableName() ).thenReturn( VARIABLE_NAME );
        Mockito.when( project.getVariableName( Mockito.any() ) ).thenReturn( VARIABLE_NAME );
        Mockito.when( project.hasBaseline() ).thenReturn( true );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );
        Mockito.when( project.getDatabase() ).thenReturn( this.wresDatabase );
        Mockito.when( project.getFeaturesCache() ).thenReturn( this.featuresCache );
        Mockito.when( project.getEnsemblesCache() ).thenReturn( this.ensemblesCache );

        // Create the factory instance
        UnitMapper unitMapper = UnitMapper.of( this.measurementUnitsCache, UNIT );
        this.factoryToTest = EnsembleRetrieverFactory.of( project, unitMapper );
    }

    /**
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     * 
     * @throws SQLException if the detailed set-up fails
     */

    private void addTimeSeriesToDatabase() throws SQLException
    {
        DataSource leftData = RetrieverTestData.generateDataSource( DatasourceType.OBSERVATIONS );
        DataSource rightData = RetrieverTestData.generateDataSource( DatasourceType.ENSEMBLE_FORECASTS );
        DataSource baselineData = RetrieverTestData.generateBaselineDataSource( DatasourceType.ENSEMBLE_FORECASTS );
        LOGGER.info( "leftData: {}", leftData );
        LOGGER.info( "rightData: {}", rightData );
        LOGGER.info( "baselineData: {}", rightData );
        ProjectConfig.Inputs fakeInputs = new ProjectConfig.Inputs( leftData.getContext(),
                                                                    rightData.getContext(),
                                                                    (DataSourceBaselineConfig) baselineData.getContext() );
        ProjectConfig fakeConfig = new ProjectConfig( fakeInputs, null, null, null, null, null );
        TimeSeries<Ensemble> timeSeriesOne = RetrieverTestData.generateTimeSeriesEnsembleOne( T0 );
        TimeSeriesIngester ingesterOne = TimeSeriesIngester.of( this.mockSystemSettings,
                                                                this.wresDatabase,
                                                                this.featuresCache,
                                                                this.timeScalesCache,
                                                                this.ensemblesCache,
                                                                this.measurementUnitsCache,
                                                                fakeConfig,
                                                                rightData,
                                                                this.lockManager,
                                                                timeSeriesOne );
        IngestResult ingestResultOne = ingesterOne.ingest()
                                                  .get( 0 );
        TimeSeriesIngester ingesterTwo = TimeSeriesIngester.of( this.mockSystemSettings,
                                                                this.wresDatabase,
                                                                this.featuresCache,
                                                                this.timeScalesCache,
                                                                this.ensemblesCache,
                                                                this.measurementUnitsCache,
                                                                fakeConfig,
                                                                baselineData,
                                                                this.lockManager,
                                                                timeSeriesOne );

        IngestResult ingestResultTwo = ingesterTwo.ingest()
                                                  .get( 0 );
        TimeSeries<Double> timeSeriesTwo = RetrieverTestData.generateTimeSeriesDoubleWithNoReferenceTimes();

        TimeSeriesIngester ingesterThree = TimeSeriesIngester.of( this.mockSystemSettings,
                                                                  this.wresDatabase,
                                                                  this.featuresCache,
                                                                  this.timeScalesCache,
                                                                  this.ensemblesCache,
                                                                  this.measurementUnitsCache,
                                                                  fakeConfig,
                                                                  leftData,
                                                                  this.lockManager,
                                                                  timeSeriesTwo );
        IngestResult ingestResultThree = ingesterThree.ingest()
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
        Project project = Projects.getProjectFromIngest( this.mockSystemSettings,
                                                         this.wresDatabase,
                                                         this.featuresCache,
                                                         this.mockExecutor,
                                                         fakeConfig,
                                                         results );
        assertTrue( project.performedInsert() );
    }

}

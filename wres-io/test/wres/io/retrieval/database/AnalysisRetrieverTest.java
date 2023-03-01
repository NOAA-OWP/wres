package wres.io.retrieval.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static wres.statistics.generated.ReferenceTime.ReferenceTimeType.*;
import static wres.io.retrieval.database.RetrieverTestConstants.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
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

import wres.config.generated.DatasourceType;
import wres.config.generated.NamedFeature;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.TestDatabase;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.project.Project;
import wres.io.project.Projects;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.retrieval.DuplicatePolicy;
import wres.io.retrieval.Retriever;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link AnalysisRetriever}.
 * @author James Brown
 */

public class AnalysisRetrieverTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( AnalysisRetrieverTest.class );

    // Comparator for ordering time-series by reference time
    private final Comparator<TimeSeries<Double>> comparator =
            ( a, b ) -> a.getReferenceTimes()
                         .get( ANALYSIS_START_TIME )
                         .compareTo( b.getReferenceTimes()
                                      .get( ANALYSIS_START_TIME ) );
    @Mock
    private SystemSettings mockSystemSettings;
    private wres.io.database.Database wresDatabase;
    @Mock
    private ProjectConfig mockProjectConfig;
    private DatabaseCaches caches;
    private DatabaseLockManager lockManager;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;

    /**
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.RIGHT;

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

        // Create the connection and schema, set up mock system settings
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockSystemSettings.getDatabaseMaximumPoolSize() )
               .thenReturn( 10 );
        Mockito.when( this.mockSystemSettings.maximumThreadCount() )
               .thenReturn( 7 );
        Mockito.when( this.mockSystemSettings.getMaximumIngestThreads() )
               .thenReturn( 7 );
        PairConfig pairConfig = Mockito.mock( PairConfig.class );
        Mockito.when( pairConfig.getGridSelection() )
               .thenReturn( List.of() );

        Mockito.when( this.mockProjectConfig.getPair() )
               .thenReturn( pairConfig );


        this.wresDatabase = new wres.io.database.Database( this.mockSystemSettings );
        this.lockManager = new DatabaseLockManagerNoop();
        this.caches = DatabaseCaches.of( this.wresDatabase, this.mockProjectConfig );

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addThreeAnalysisTimeSeriesToTheDatabase();
    }

    @Test
    public void testRetrievalOfThreeOverlappingAnalysisTimeSeriesWithDuplicatesRemoved()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> analysisRetriever =
                new AnalysisRetriever.Builder().setDuplicatePolicy( DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME )
                                               // Weird that I cannot set this
                                               // before setDuplicatePolicy.
                                               // Composition over inheritance?
                                               .setDatabase( this.wresDatabase )
                                               .setProjectId( PROJECT_ID )
                                               .setFeaturesCache( this.caches.getFeaturesCache() )
                                               .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                               .setVariableName( VARIABLE_NAME )
                                               .setFeatures( Set.of( FEATURE ) )
                                               .setLeftOrRightOrBaseline( LRB )
                                               .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = analysisRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.sorted( this.comparator )
                                                                  .toList();

        // There are three time-series, so assert that
        assertEquals( 3, actualCollection.size() );
        TimeSeries<Double> actualSeriesOne = actualCollection.get( 0 );
        TimeSeries<Double> actualSeriesTwo = actualCollection.get( 1 );
        TimeSeries<Double> actualSeriesThree = actualCollection.get( 2 );

        // Create the first expected series
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesOne =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T2023_04_01T01_00_00Z, 30.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeries.Builder<Double>().setMetadata( metadataTwo )
                                                .addEvent( Event.of( T2023_04_01T04_00_00Z, 72.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 79.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 86.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );

        // Create the third expected series
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesThree =
                new TimeSeries.Builder<Double>().setMetadata( metadataThree )
                                                .addEvent( Event.of( T2023_04_01T07_00_00Z, 114.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 121.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T09:00:00Z" ), 128.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T10:00:00Z" ), 135.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T11:00:00Z" ), 142.0 ) )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T12:00:00Z" ), 149.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesThree, actualSeriesThree );

    }

    @Test
    public void testRetrievalOfAnalysisTimeSeriesWithAnalysisDurationOfPT1H()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> analysisRetriever =
                new AnalysisRetriever.Builder().setLatestAnalysisDuration( Duration.ofHours( 1 ) )
                                               .setDatabase( this.wresDatabase )
                                               .setFeaturesCache( this.caches.getFeaturesCache() )
                                               .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                               .setProjectId( PROJECT_ID )
                                               .setVariableName( VARIABLE_NAME )
                                               .setFeatures( Set.of( FEATURE ) )
                                               .setLeftOrRightOrBaseline( LRB )
                                               .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = analysisRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.sorted( this.comparator )
                                                                  .toList();

        // There are three time-series, so assert that
        assertEquals( 3, actualCollection.size() );
        TimeSeries<Double> actualSeriesOne = actualCollection.get( 0 );
        TimeSeries<Double> actualSeriesTwo = actualCollection.get( 1 );
        TimeSeries<Double> actualSeriesThree = actualCollection.get( 2 );

        // Create the first expected series
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesOne =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T2023_04_01T01_00_00Z, 30.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeries.Builder<Double>().setMetadata( metadataTwo )
                                                .addEvent( Event.of( T2023_04_01T04_00_00Z, 72.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );

        // Create the third expected series
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesThree =
                new TimeSeries.Builder<Double>().setMetadata( metadataThree )
                                                .addEvent( Event.of( T2023_04_01T07_00_00Z, 114.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesThree, actualSeriesThree );

    }

    @Test
    public void testRetrievalOfAnalysisTimeSeriesWithAnalysisDurationOfPT1HAndPT2H()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> analysisRetriever =
                new AnalysisRetriever.Builder().setEarliestAnalysisDuration( Duration.ofHours( 0 ) )
                                               .setLatestAnalysisDuration( Duration.ofHours( 2 ) )
                                               .setDatabase( this.wresDatabase )
                                               .setFeaturesCache( this.caches.getFeaturesCache() )
                                               .setMeasurementUnitsCache( this.caches.getMeasurementUnitsCache() )
                                               .setProjectId( PROJECT_ID )
                                               .setVariableName( VARIABLE_NAME )
                                               .setFeatures( Set.of( FEATURE ) )
                                               .setLeftOrRightOrBaseline( LRB )
                                               .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = analysisRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.sorted( this.comparator )
                                                                  .toList();

        // There are three time-series, so assert that
        assertEquals( 6, actualCollection.size() );
        TimeSeries<Double> actualSeriesOne = actualCollection.get( 0 );
        TimeSeries<Double> actualSeriesTwo = actualCollection.get( 1 );
        TimeSeries<Double> actualSeriesThree = actualCollection.get( 2 );
        TimeSeries<Double> actualSeriesFour = actualCollection.get( 3 );
        TimeSeries<Double> actualSeriesFive = actualCollection.get( 4 );
        TimeSeries<Double> actualSeriesSix = actualCollection.get( 5 );

        // Create the first expected series
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesOne =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                .addEvent( Event.of( T2023_04_01T01_00_00Z, 30.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeries.Builder<Double>().setMetadata( metadataTwo )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );

        // Create the third expected series
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesThree =
                new TimeSeries.Builder<Double>().setMetadata( metadataThree )
                                                .addEvent( Event.of( T2023_04_01T04_00_00Z, 72.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesThree, actualSeriesThree );

        // Create the fourth expected series
        TimeSeriesMetadata metadataFour =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesFour =
                new TimeSeries.Builder<Double>().setMetadata( metadataFour )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 79.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesFour, actualSeriesFour );

        // Create the fifth expected series
        TimeSeriesMetadata metadataFive =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesFive =
                new TimeSeries.Builder<Double>().setMetadata( metadataFive )
                                                .addEvent( Event.of( T2023_04_01T07_00_00Z, 114.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesFive, actualSeriesFive );

        // Create the sixth expected series
        TimeSeriesMetadata metadataSix =
                TimeSeriesMetadata.of( Map.of( ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       UNIT );
        TimeSeries<Double> expectedSeriesSix =
                new TimeSeries.Builder<Double>().setMetadata( metadataSix )
                                                .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 121.0 ) )
                                                .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesSix, actualSeriesSix );

    }

    @After
    public void tearDown() throws SQLException
    {
        this.dropTheTablesAndSchema();
        this.testDatabase.shutdown( this.rawConnection );
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
     * Performs the detailed set-up work to add three time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     *
     * @throws SQLException if the detailed set-up fails
     */

    private void addThreeAnalysisTimeSeriesToTheDatabase() throws SQLException
    {
        DataSource leftData = RetrieverTestData.generateDataSource( LeftOrRightOrBaseline.LEFT,
                                                                    DatasourceType.OBSERVATIONS );
        DataSource rightData = RetrieverTestData.generateDataSource( LeftOrRightOrBaseline.RIGHT,
                                                                     DatasourceType.ANALYSES );
        LOGGER.info( "leftData: {}", leftData );
        LOGGER.info( "rightData: {}", rightData );
        ProjectConfig.Inputs fakeInputs =
                new ProjectConfig.Inputs( leftData.getContext(), rightData.getContext(), null );
        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                List.of( new NamedFeature( FEATURE.getName(),
                                                                           FEATURE.getName(),
                                                                           null ) ),
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
        ProjectConfig fakeConfig = new ProjectConfig( fakeInputs, pairConfig, null, null, null, null );
        TimeSeries<Double> timeSeriesOne = RetrieverTestData.generateTimeSeriesDoubleOne( ANALYSIS_START_TIME );
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
        TimeSeries<Double> timeSeriesTwo = RetrieverTestData.generateTimeSeriesDoubleTwo( ANALYSIS_START_TIME );

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
        TimeSeries<Double> timeSeriesThree = RetrieverTestData.generateTimeSeriesDoubleThree( ANALYSIS_START_TIME );

        TimeSeriesIngester ingesterThree =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setProjectConfig( fakeConfig )
                                                        .setLockManager( this.lockManager )
                                                        .build();
        Stream<TimeSeriesTuple> tupleStreamThree =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesThree, rightData ) );
        IngestResult ingestResultThree = ingesterThree.ingest( tupleStreamThree, rightData )
                                                      .get( 0 );

        TimeSeries<Double> timeSeriesFour = RetrieverTestData.generateTimeSeriesDoubleWithNoReferenceTimes();
        Stream<TimeSeriesTuple> tupleStreamFour =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesFour, leftData ) );
        TimeSeriesIngester ingesterFour =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setCaches( this.caches )
                                                        .setProjectConfig( fakeConfig )
                                                        .setLockManager( this.lockManager )
                                                        .build();

        IngestResult ingestResultFour = ingesterFour.ingest( tupleStreamFour, leftData )
                                                    .get( 0 );

        List<IngestResult> results = List.of( ingestResultOne,
                                              ingestResultTwo,
                                              ingestResultThree,
                                              ingestResultFour );

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
                                                         null,
                                                         fakeConfig,
                                                         results );
        assertTrue( project.save() );
    }

}

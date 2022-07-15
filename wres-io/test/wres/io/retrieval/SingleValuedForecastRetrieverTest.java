package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static wres.config.generated.LeftOrRightOrBaseline.RIGHT;
import static wres.datamodel.time.ReferenceTimeType.T0;
import static wres.io.retrieval.RetrieverTestConstants.*;

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
import java.util.stream.Collectors;
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

import wres.config.generated.DatasourceType;
import wres.config.generated.PairConfig;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.ProjectConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.data.caching.Caches;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.project.Project;
import wres.io.project.Projects;
import wres.io.reading.DataSource;
import wres.io.utilities.TestDatabase;
import wres.statistics.generated.TimeWindow;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedForecastRetriever}.
 * @author James Brown
 */

public class SingleValuedForecastRetrieverTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedForecastRetrieverTest.class );
    @Mock private SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;
    @Mock private Executor mockExecutor;
    @Mock private ProjectConfig mockProjectConfig;
    private Caches caches;
    private DatabaseLockManager lockManager;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;


    /**
     * The unit mapper.
     */

    private UnitMapper unitMapper;

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

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockSystemSettings.getMaximumPoolSize() )
               .thenReturn( 10 );
        PairConfig pairConfig = Mockito.mock( PairConfig.class );
        Mockito.when( pairConfig.getGridSelection() )
               .thenReturn( List.of() );
        Mockito.when( this.mockProjectConfig.getPair() )
               .thenReturn( pairConfig );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        this.caches = Caches.of( this.wresDatabase, this.mockProjectConfig );
        this.lockManager = new DatabaseLockManagerNoop();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase();

        // Create the unit mapper
        this.unitMapper = UnitMapper.of( this.caches.getMeasurementUnitsCache(), UNIT );
    }

    @Test
    public void testRetrievalOfTwoForecastTimeSeriesEachWithFiveEvents()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.caches.getFeaturesCache() )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setUnitMapper( this.unitMapper )
                                                           .setLeftOrRightOrBaseline(
                                                                                      RIGHT )
                                                           .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = forecastRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.collect( Collectors.toList() );

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
        Instant referenceEnd = T2023_04_01T19_00_00Z;
        Instant validStart = Instant.parse( "2023-04-01T03:00:00Z" );
        Instant validEnd = T2023_04_01T19_00_00Z;
        Duration leadStart = Duration.ofHours( 1 );
        Duration leadEnd = Duration.ofHours( 4 );

        TimeWindow inner = MessageFactory.getTimeWindow( referenceStart,
                                                         referenceEnd,
                                                         validStart,
                                                         validEnd,
                                                         leadStart,
                                                         leadEnd );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.caches.getFeaturesCache() )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setUnitMapper( this.unitMapper )
                                                           .setTimeWindow( timeWindow )
                                                           .setLeftOrRightOrBaseline( RIGHT )
                                                           .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = forecastRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.collect( Collectors.toList() );

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
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setLeftOrRightOrBaseline( RIGHT )
                                                           .setUnitMapper( this.unitMapper )
                                                           .build();

        // Get the time-series
        List<Long> identifiers = forecastRetriever.getAllIdentifiers().boxed().collect( Collectors.toList() );

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
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeatures( Set.of( FEATURE ) )
                                                           .setUnitMapper( this.unitMapper )
                                                           .setLeftOrRightOrBaseline( RIGHT )
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

    private void addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase() throws SQLException
    {
        DataSource leftData = RetrieverTestData.generateDataSource( DatasourceType.OBSERVATIONS );
        DataSource rightData = RetrieverTestData.generateDataSource( DatasourceType.SINGLE_VALUED_FORECASTS );
        LOGGER.info( "leftData: {}", leftData );
        LOGGER.info( "rightData: {}", rightData );
        ProjectConfig.Inputs fakeInputs =
                new ProjectConfig.Inputs( leftData.getContext(), rightData.getContext(), null );
        ProjectConfig fakeConfig = new ProjectConfig( fakeInputs, null, null, null, null, null );
        TimeSeries<Double> timeSeriesOne = RetrieverTestData.generateTimeSeriesDoubleOne( T0 );
        TimeSeriesIngester ingesterOne = new TimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                                         .setDatabase( this.wresDatabase )
                                                                         .setCaches( this.caches )
                                                                         .setProjectConfig( fakeConfig )
                                                                         .setLockManager( this.lockManager )
                                                                         .build();
        IngestResult ingestResultOne = ingesterOne.ingestSingleValuedTimeSeries( timeSeriesOne, rightData )
                                                  .get( 0 );
        TimeSeries<Double> timeSeriesTwo = RetrieverTestData.generateTimeSeriesDoubleFour( T0 );

        TimeSeriesIngester ingesterTwo = new TimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                                         .setDatabase( this.wresDatabase )
                                                                         .setCaches( this.caches )
                                                                         .setProjectConfig( fakeConfig )
                                                                         .setLockManager( this.lockManager )
                                                                         .build();
        IngestResult ingestResultTwo = ingesterTwo.ingestSingleValuedTimeSeries( timeSeriesTwo, rightData )
                                                  .get( 0 );

        TimeSeries<Double> timeSeriesThree = RetrieverTestData.generateTimeSeriesDoubleWithNoReferenceTimes();

        TimeSeriesIngester ingesterThree = new TimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                                           .setDatabase( this.wresDatabase )
                                                                           .setCaches( this.caches )
                                                                           .setProjectConfig( fakeConfig )
                                                                           .setLockManager( this.lockManager )
                                                                           .build();
        IngestResult ingestResultThree = ingesterThree.ingestSingleValuedTimeSeries( timeSeriesThree, leftData )
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
                                                         this.caches.getFeaturesCache(),
                                                         this.mockExecutor,
                                                         fakeConfig,
                                                         results );
        assertTrue( project.performedInsert() );
    }

}

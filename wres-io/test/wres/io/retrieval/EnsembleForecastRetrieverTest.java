package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import static wres.datamodel.time.ReferenceTimeType.T0;
import static wres.io.retrieval.RetrieverTestConstants.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.TimeScales;
import wres.io.project.Project;
import wres.io.project.Projects;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.TestDatabase;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link EnsembleForecastRetriever}.
 * @author James Brown
 */

public class EnsembleForecastRetrieverTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleForecastRetrieverTest.class );
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
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.RIGHT;

    /**
     * Unit mapper.
     */

    private UnitMapper unitMapper;

    /**
     * Error message when attempting to retrieve by identifier.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of ensemble time-series by identifier is not "
                                                      + "currently possible because there is no identifier for "
                                                      + "ensemble time-series in the WRES database.";

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

        // Create a connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );

        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );

        // Create the tables
        this.addTheDatabaseAndTables();

        this.featuresCache = new Features( this.wresDatabase );
        this.measurementUnitsCache = new MeasurementUnits( this.wresDatabase );
        this.timeScalesCache = new TimeScales( this.wresDatabase );
        this.ensemblesCache = new Ensembles( this.wresDatabase );
        this.lockManager = new DatabaseLockManagerNoop();

        // Add some data for testing
        this.addOneForecastTimeSeriesWithFiveEventsAndThreeMembersToTheDatabase();

        // Create the unit mapper
        this.unitMapper = UnitMapper.of( this.measurementUnitsCache, UNIT );
    }

    @Test
    public void testRetrievalOfOneTimeSeriesWithFiveEventsAndThreeMembers()
    {
        // Build the retriever
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setEnsemblesCache( this.ensemblesCache )
                                                       .setDatabase( this.wresDatabase )
                                                       .setFeaturesCache( this.featuresCache )
                                                       .setProjectId( PROJECT_ID )
                                                       .setVariableName( VARIABLE_NAME )
                                                       .setFeatures( Set.of( FEATURE ) )
                                                       .setUnitMapper( this.unitMapper )
                                                       .setLeftOrRightOrBaseline( LRB )
                                                       .build();

        // Get the time-series
        Stream<TimeSeries<Ensemble>> forecastSeries = forecastRetriever.get();

        // Stream into a collection
        List<TimeSeries<Ensemble>> actualCollection = forecastSeries.collect( Collectors.toList() );

        // There is one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Ensemble> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       this.unitMapper.getDesiredMeasurementUnitName() );
        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<>();

        Labels expectedLabels = Labels.of( "123", "456", "567" );

        TimeSeries<Ensemble> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ),
                                            Ensemble.of( new double[] { 30.0, 100.0, 65.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ),
                                            Ensemble.of( new double[] { 37.0, 107.0, 72.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ),
                                            Ensemble.of( new double[] { 44.0, 114.0, 79.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ),
                                            Ensemble.of( new double[] { 51.0, 121.0, 86.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ),
                                            Ensemble.of( new double[] { 58.0, 128.0, 93.0 }, expectedLabels ) ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testRetrievalOfOneTimeSeriesWithFiveEventsAndOneMemberUsingEnsembleConstraints()
            throws SQLException
    {
        long include = this.ensemblesCache.getEnsembleID( "567" );
        long excludeOne = this.ensemblesCache.getEnsembleID( "123" );
        long excludeTwo = this.ensemblesCache.getEnsembleID( "456" );

        // Build the retriever with ensemble constraints
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setEnsemblesCache( this.ensemblesCache )
                                                       .setEnsembleIdsToInclude( Set.of( include ) )
                                                       .setEnsembleIdsToExclude( Set.of( excludeOne, excludeTwo ) )
                                                       .setDatabase( this.wresDatabase )
                                                       .setFeaturesCache( this.featuresCache )
                                                       .setProjectId( PROJECT_ID )
                                                       .setVariableName( VARIABLE_NAME )
                                                       .setFeatures( Set.of( FEATURE ) )
                                                       .setUnitMapper( this.unitMapper )
                                                       .setLeftOrRightOrBaseline( LRB )
                                                       .build();

        // Get the time-series
        Stream<TimeSeries<Ensemble>> forecastSeries = forecastRetriever.get();

        // Stream into a collection
        List<TimeSeries<Ensemble>> actualCollection = forecastSeries.collect( Collectors.toList() );

        // There is one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Ensemble> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       this.unitMapper.getDesiredMeasurementUnitName() );
        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<>();

        Labels expectedLabels = Labels.of( "567" );

        TimeSeries<Ensemble> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ),
                                            Ensemble.of( new double[] { 65.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ),
                                            Ensemble.of( new double[] { 72.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ),
                                            Ensemble.of( new double[] { 79.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ),
                                            Ensemble.of( new double[] { 86.0 }, expectedLabels ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ),
                                            Ensemble.of( new double[] { 93.0 }, expectedLabels ) ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetAllIdentifiersThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setEnsemblesCache( this.ensemblesCache )
                                                       .setDatabase( this.wresDatabase )
                                                       .setProjectId( PROJECT_ID )
                                                       .setVariableName( VARIABLE_NAME )
                                                       .setFeatures( Set.of( FEATURE ) )
                                                       .setUnitMapper( this.unitMapper )
                                                       .setLeftOrRightOrBaseline( LRB )
                                                       .build();

        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               forecastRetriever::getAllIdentifiers );

        assertEquals( NO_IDENTIFIER_ERROR, expected.getMessage() );
    }

    @Test
    public void testGetByIdentifierThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setEnsemblesCache( this.ensemblesCache )
                                                       .setDatabase( this.wresDatabase )
                                                       .setProjectId( PROJECT_ID )
                                                       .setVariableName( VARIABLE_NAME )
                                                       .setFeatures( Set.of( FEATURE ) )
                                                       .setUnitMapper( this.unitMapper )
                                                       .setLeftOrRightOrBaseline( LRB )
                                                       .build();

        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               () -> forecastRetriever.get( 123 ) );

        assertEquals( NO_IDENTIFIER_ERROR, expected.getMessage() );
    }

    @Test
    public void testGetByIdentifierStreamThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setEnsemblesCache( this.ensemblesCache )
                                                       .setDatabase( this.wresDatabase )
                                                       .setProjectId( PROJECT_ID )
                                                       .setVariableName( VARIABLE_NAME )
                                                       .setFeatures( Set.of( FEATURE ) )
                                                       .setUnitMapper( this.unitMapper )
                                                       .setLeftOrRightOrBaseline( LRB )
                                                       .build();

        LongStream longStream = LongStream.of();
        
        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               () -> forecastRetriever.get( longStream ) );

        assertEquals( NO_IDENTIFIER_ERROR, expected.getMessage() );
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
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     * 
     * @throws SQLException if the detailed set-up fails
     */

    private void addOneForecastTimeSeriesWithFiveEventsAndThreeMembersToTheDatabase() throws SQLException
    {
        DataSource leftData = RetrieverTestData.generateDataSource( DatasourceType.OBSERVATIONS );
        DataSource rightData = RetrieverTestData.generateDataSource( DatasourceType.ENSEMBLE_FORECASTS );
        LOGGER.info( "leftData: {}", leftData );
        LOGGER.info( "rightData: {}" , rightData );
        ProjectConfig.Inputs fakeInputs = new ProjectConfig.Inputs( leftData.getContext(), rightData.getContext(), null );
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
        IngestResult ingestResultOne = ingesterOne.call()
                                                  .get( 0 );

        TimeSeries<Double> timeSeriesTwo = RetrieverTestData.generateTimeSeriesDoubleWithNoReferenceTimes();

        TimeSeriesIngester ingesterTwo = TimeSeriesIngester.of( this.mockSystemSettings,
                                                                  this.wresDatabase,
                                                                  this.featuresCache,
                                                                  this.timeScalesCache,
                                                                  this.ensemblesCache,
                                                                  this.measurementUnitsCache,
                                                                  fakeConfig,
                                                                  leftData,
                                                                  this.lockManager,
                                                                  timeSeriesTwo );
        IngestResult ingestResultTwo = ingesterTwo.call()
                                                    .get( 0 );

        List<IngestResult> results = List.of( ingestResultOne,
                                              ingestResultTwo );

        try ( Statement statement = this.rawConnection.createStatement() )
        {
            ResultSet sourceData = statement.executeQuery( "select source_id, hash, measurementunit_id, path from wres.source" );

            while ( sourceData.next() )
            {
                LOGGER.info( "source_id={} hash={} measurementunit_id={} path={}",
                             sourceData.getLong( "source_id" ),
                             sourceData.getString( "hash"),
                             sourceData.getShort( "measurementunit_id" ),
                             sourceData.getString( "path" ) );
            }
        }

        LOGGER.info( "ingestResultOne: {}", ingestResultOne );
        LOGGER.info( "ingestResultTwo: {}", ingestResultTwo );
        Project project = Projects.getProjectFromIngest( this.mockSystemSettings,
                                                         this.wresDatabase,
                                                         this.featuresCache,
                                                         this.mockExecutor,
                                                         fakeConfig,
                                                         results );
        assertTrue( project.performedInsert() );
    }

}

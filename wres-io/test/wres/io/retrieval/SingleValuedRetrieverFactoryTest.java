package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static wres.io.retrieval.RetrieverTestConstants.*;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
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

import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.project.Project;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedRetrieverFactory}.
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedRetrieverFactoryTest
{
    @Mock
    private SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;
    @Mock
    private Executor mockExecutor;
    private Features featuresCache;
    private MeasurementUnits measurementUnitsCache;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;

    /**
     * The measurement units for testing.
     */

    private static final String CFS = "CFS";

    /**
     * The feature name.
     */

    private static final FeatureKey FAKE_FEATURE = FeatureKey.of( "FAKE" );

    /**
     * The variable name.
     */

    private static final String STREAMFLOW = "streamflow";

    // Times for re-use
    private static final String T2023_04_01T17_00_00Z = "2023-04-01T17:00:00Z";
    private static final String T2023_04_01T00_00_00Z = "2023-04-01T00:00:00Z";
    private static final String T2023_04_01T07_00_00Z = "2023-04-01T07:00:00Z";
    private static final String T2023_04_01T04_00_00Z = "2023-04-01T04:00:00Z";
    private static final String T2023_04_01T03_00_00Z = "2023-04-01T03:00:00Z";
    private static final String T2023_04_01T02_00_00Z = "2023-04-01T02:00:00Z";
    private static final String T2023_04_01T01_00_00Z = "2023-04-01T01:00:00Z";

    /**
     * Insert statement for re-use.
     */

    private static final String INSERT_INTO_WRES_PROJECT_SOURCE = "INSERT INTO wres.ProjectSource (project_id, "
                                                                  + "source_id, member) VALUES ({0},{1},''{2}'')";

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
        this.testDatabase = new TestDatabase( "SingleValuedRetrieverFactoryTest" );
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
               .thenReturn( "h2" );
        Mockito.when( this.mockSystemSettings.getMaximumPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        this.featuresCache = new Features( this.wresDatabase );
        this.measurementUnitsCache = new MeasurementUnits( this.wresDatabase );

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase();
        this.addAnObservedTimeSeriesWithTenEventsToTheDatabase();

        // Create the retriever factory to test
        this.createSingleValuedRetrieverFactory();
    }

    @Test
    public void testGetLeftRetrieverReturnsOneTimeSeriesWithTenEvents()
    {

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( Set.of( FAKE_FEATURE ) )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap(),
                                       TimeScaleOuter.of(),
                                       STREAMFLOW,
                                       FAKE_FEATURE,
                                       "CFS" );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T01_00_00Z ), 30.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T07_00_00Z ), 72.0 ) )
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
        TimeWindowOuter timeWindow = TimeWindowOuter.of( Instant.parse( T2023_04_01T02_00_00Z ),
                                                         Instant.parse( T2023_04_01T07_00_00Z ) );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( Set.of( FAKE_FEATURE ), 
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
                                       STREAMFLOW,
                                       FAKE_FEATURE,
                                       "CFS" );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T07_00_00Z ), 72.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetRightRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindowOuter timeWindow = TimeWindowOuter.of( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                         Instant.parse( T2023_04_01T00_00_00Z ),
                                                         Instant.parse( T2023_04_01T01_00_00Z ),
                                                         Instant.parse( T2023_04_01T04_00_00Z ) );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getRightRetriever( Set.of( FAKE_FEATURE ), 
                                                                                          timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
                                       TimeScaleOuter.of(),
                                       STREAMFLOW,
                                       FAKE_FEATURE,
                                       "CFS" );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetBaselineRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindowOuter timeWindow = TimeWindowOuter.of( Instant.parse( "2023-03-31T11:00:00Z" ),
                                                         Instant.parse( T2023_04_01T00_00_00Z ),
                                                         Instant.parse( T2023_04_01T01_00_00Z ),
                                                         Instant.parse( T2023_04_01T04_00_00Z ) );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getBaselineRetriever( Set.of( FAKE_FEATURE ), 
                                                                                             timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
                                       TimeScaleOuter.of(),
                                       STREAMFLOW,
                                       FAKE_FEATURE,
                                       "CFS" );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
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

        this.testDatabase.createMeasurementUnitTable( liquibaseDatabase );
        this.testDatabase.createSourceTable( liquibaseDatabase );
        this.testDatabase.createProjectTable( liquibaseDatabase );
        this.testDatabase.createProjectSourceTable( liquibaseDatabase );
        this.testDatabase.createFeatureTable( liquibaseDatabase );
        this.testDatabase.createEnsembleTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesValueTable( liquibaseDatabase );
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
     * @throws SQLException if the factory could not be created
     */

    private void createSingleValuedRetrieverFactory() throws SQLException
    {
        // Mock the sufficient elements of the ProjectConfig
        PairConfig pairsConfig = new PairConfig( CFS,
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
                                                 null,
                                                 null );
        List<DataSourceConfig.Source> sourceList = new ArrayList<>();

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      sourceList,
                                                      new Variable( STREAMFLOW, null ),
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
                                              new Variable( STREAMFLOW, null ),
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

        FeatureTuple featureTuple = new FeatureTuple( FAKE_FEATURE, FAKE_FEATURE, null );
        Set<FeatureTuple> allFeatures = Set.of( featureTuple );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( PROJECT_ID );
        Mockito.when( project.getFeatures() ).thenReturn( allFeatures );
        Mockito.when( project.getVariableName( Mockito.any( LeftOrRightOrBaseline.class ) ) ).thenReturn( STREAMFLOW );
        Mockito.when( project.hasBaseline() ).thenReturn( true );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );
        Mockito.when( project.getDatabase() ).thenReturn( this.wresDatabase );
        Mockito.when( project.getFeaturesCache() ).thenReturn( this.featuresCache );

        // Create the factory instance
        UnitMapper unitMapper = UnitMapper.of( this.measurementUnitsCache, CFS );
        this.factoryToTest = SingleValuedRetrieverFactory.of( project,
                                                              unitMapper );
    }

    /**
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     * 
     * @throws SQLException if the detailed set-up fails
     */

    private void addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase() throws SQLException
    {
        // Add a source
        SourceDetails.SourceKey sourceKey = SourceDetails.createKey( URI.create( "/this/is/just/a/test" ),
                                                                     "2017-06-16 11:13:00",
                                                                     null,
                                                                     "abc123" );

        SourceDetails sourceDetails = new SourceDetails( sourceKey );

        sourceDetails.save( this.wresDatabase );

        assertTrue( sourceDetails.performedInsert() );

        Long sourceId = sourceDetails.getId();

        assertNotNull( sourceId );

        // Add a project 
        Project project =
                new Project( this.mockSystemSettings,
                             this.wresDatabase,
                             this.featuresCache,
                             this.mockExecutor,
                             new ProjectConfig( null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                "test_project" ),
                             PROJECT_HASH );
        project.save();

        assertTrue( project.performedInsert() );

        assertEquals( PROJECT_HASH, project.getHash() );

        // Add a project source
        // There is no wres abstraction to help with this
        String projectSourceInsert = INSERT_INTO_WRES_PROJECT_SOURCE;

        // Project source for RIGHT
        projectSourceInsert = MessageFormat.format( projectSourceInsert,
                                                    project.getId(),
                                                    sourceId,
                                                    LeftOrRightOrBaseline.RIGHT.value() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Project source for BASELINE - same source for simplicity
        String projectSourceBaselineInsert = INSERT_INTO_WRES_PROJECT_SOURCE;

        projectSourceBaselineInsert = MessageFormat.format( projectSourceBaselineInsert,
                                                            project.getId(),
                                                            sourceId,
                                                            LeftOrRightOrBaseline.BASELINE.value() );

        DataScripter scriptBaseline = new DataScripter( this.wresDatabase,
                                                        projectSourceBaselineInsert );
        int rowsBaseline = scriptBaseline.execute();

        assertEquals( 1, rowsBaseline );

        // Add a feature
        FeatureDetails feature = new FeatureDetails( FAKE_FEATURE );
        feature.save( this.wresDatabase );

        assertNotNull( feature.getId() );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( CFS );
        measurement.save( this.wresDatabase );
        Long measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        EnsembleDetails ensemble = new EnsembleDetails();
        ensemble.setEnsembleName( "ENS123" );
        ensemble.save( this.wresDatabase );
        Long ensembleId = ensemble.getId();

        assertNotNull( ensembleId );

        // Add two forecasts
        // There is an abstraction to help with this, namely wres.io.data.details.TimeSeries, but the resulting 
        // prepared statement fails on wres.TimeSeriesSource, seemingly on the datatype of the timeseries_id column, 
        // although H2 reported the expected type. See #56214-102        

        // Two reference times, PT17H apart
        Instant firstReference = Instant.parse( T2023_04_01T00_00_00Z );
        Instant secondReference = Instant.parse( T2023_04_01T17_00_00Z );

        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     firstReference,
                                                     sourceId,
                                                     STREAMFLOW,
                                                     feature.getId() );
        firstTraceRow.setTimeScale( timeScale );
        long firstTraceRowId = firstTraceRow.getTimeSeriesID();


        wres.io.data.details.TimeSeries secondTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     secondReference,
                                                     sourceId,
                                                     STREAMFLOW,
                                                     feature.getId() );
        secondTraceRow.setTimeScale( timeScale );
        long secondTraceRowId = secondTraceRow.getTimeSeriesID();

        // Add the time-series values to wres.TimeSeriesValue       
        Duration seriesIncrement = Duration.ofHours( 1 );
        double valueStart = 23.0;
        double valueIncrement = 7.0;

        // Insert template
        // As above, this does not work as a prepared statement via DataScripter
        String forecastInsert =
                "INSERT INTO wres.TimeSeriesValue (timeseries_id, lead, series_value) VALUES ({0},{1},{2})";

        // Insert the time-series values into the db
        double forecastValue = valueStart;
        Map<Long, Instant> series = new TreeMap<>();
        series.put( firstTraceRowId, firstReference );
        series.put( secondTraceRowId, secondReference );

        // Iterate and add the series values
        for ( Map.Entry<Long, Instant> nextSeries : series.entrySet() )
        {
            Instant validTime = nextSeries.getValue();

            for ( long i = 0; i < 5; i++ )
            {
                // Increment the valid datetime and value
                validTime = validTime.plus( seriesIncrement );
                forecastValue = forecastValue + valueIncrement;
                int lead = (int) seriesIncrement.multipliedBy( i + 1 ).toMinutes();

                // Insert
                String insert = MessageFormat.format( forecastInsert,
                                                      nextSeries.getKey(),
                                                      lead,
                                                      forecastValue );

                DataScripter forecastScript = new DataScripter( this.wresDatabase,
                                                                insert );

                int row = forecastScript.execute();

                // One row added
                assertEquals( 1, row );
            }
        }
    }

    /**
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     * 
     * @throws SQLException if the detailed set-up fails
     */

    private void addAnObservedTimeSeriesWithTenEventsToTheDatabase() throws SQLException
    {
        // Add a source
        SourceDetails.SourceKey sourceKey = SourceDetails.createKey( URI.create( "/this/is/just/a/test" ),
                                                                     "2017-06-16 11:13:00",
                                                                     null,
                                                                     "def456" );

        SourceDetails sourceDetails = new SourceDetails( sourceKey );

        sourceDetails.save( this.wresDatabase );

        assertTrue( sourceDetails.performedInsert() );

        Long sourceId = sourceDetails.getId();

        assertNotNull( sourceId );

        // Add a project source
        // There is no wres abstraction to help with this
        String projectSourceInsert = INSERT_INTO_WRES_PROJECT_SOURCE;

        //Format 
        projectSourceInsert = MessageFormat.format( projectSourceInsert,
                                                    PROJECT_ID,
                                                    sourceId,
                                                    LeftOrRightOrBaseline.LEFT.value() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( CFS );
        measurement.save( this.wresDatabase );
        Long measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        EnsembleDetails ensemble = new EnsembleDetails();
        ensemble.setEnsembleName( "ENS123" );
        ensemble.save( this.wresDatabase );
        Long ensembleId = ensemble.getId();

        assertNotNull( ensembleId );

        Instant latestObsDatetime = Instant.parse( "2023-04-01T10:00:00Z" );
        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     latestObsDatetime,
                                                     sourceId,
                                                     STREAMFLOW,
                                                     1 );
        firstTraceRow.setTimeScale( timeScale );
        long firstTraceRowId = firstTraceRow.getTimeSeriesID();

        // Add some observations
        // There is no wres abstraction to help with this

        Instant seriesStart = Instant.parse( T2023_04_01T00_00_00Z );
        Duration seriesIncrement = Duration.ofHours( 1 );
        double valueStart = 23.0;
        double valueIncrement = 7.0;

        String observationInsert =
                "INSERT INTO wres.TimeSeriesValue (timeseries_id, lead, series_value) VALUES ({0},{1},{2})";

        // Insert 10 observed events into the db
        Instant observationTime = seriesStart;
        double observedValue = valueStart;
        for ( int i = 0; i < 10; i++ )
        {
            // Increment the valid datetime and value
            observationTime = observationTime.plus( seriesIncrement );
            observedValue = observedValue + valueIncrement;

            // Insert
            String insert = MessageFormat.format( observationInsert,
                                                  firstTraceRowId,
                                                  Duration.between( latestObsDatetime,
                                                                    observationTime )
                                                          .toMinutes(),
                                                  observedValue );

            DataScripter observedScript = new DataScripter( this.wresDatabase,
                                                            insert );

            int row = observedScript.execute();

            // One row added
            assertEquals( 1, row );
        }

    }

}

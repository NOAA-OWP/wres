package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.data.caching.Features;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.project.Project;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link EnsembleRetrieverFactory}.
 * @author james.brown@hydrosolved.com
 */

public class EnsembleRetrieverFactoryTest
{
    @Mock private SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;
    @Mock private Executor mockExecutor;
    private Features featuresCache;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;

    /**
     * A project_id for testing;
     */

    private static final Integer PROJECT_ID = 1;

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
        MockitoAnnotations.initMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "EnsembleRetrieverFactoryTest" );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        this.featuresCache = new Features( this.wresDatabase );

        // Create the connection and schema, set up mock settings
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase();
        this.addAnObservedTimeSeriesWithTenEventsToTheDatabase();

        // Create the retriever factory to test
        this.createEnsembleRetrieverFactory();
    }

    @Test
    public void testGetLeftRetrieverReturnsOneTimeSeriesWithTenEvents()
    {

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever()
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
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
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
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( timeWindow )
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
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
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
        List<TimeSeries<Ensemble>> actualCollection = this.factoryToTest.getRightRetriever( timeWindow )
                                                                        .get()
                                                                        .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Ensemble> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
                                       TimeScaleOuter.of(),
                                       STREAMFLOW,
                                       FAKE_FEATURE,
                                       "CFS" );
        TimeSeriesBuilder<Ensemble> builder = new TimeSeriesBuilder<>();
        TimeSeries<Ensemble> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), Ensemble.of( 37.0, 107.0, 72.0) ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), Ensemble.of( 44.0, 114.0, 79.0) ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), Ensemble.of( 51.0, 121.0, 86.0 ) ) )
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
        List<TimeSeries<Ensemble>> actualCollection = this.factoryToTest.getBaselineRetriever( timeWindow )
                                                                        .get()
                                                                        .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Ensemble> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
                                       TimeScaleOuter.of(),
                                       STREAMFLOW,
                                       FAKE_FEATURE,
                                       "CFS" );
        TimeSeriesBuilder<Ensemble> builder = new TimeSeriesBuilder<>();
        TimeSeries<Ensemble> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), Ensemble.of( 37.0, 107.0, 72.0  ) ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), Ensemble.of( 44.0, 114.0, 79.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), Ensemble.of( 51.0, 121.0, 86.0 ) ) )
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
     * @throws SQLException 
     * 
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

        this.testDatabase.createMeasurementUnitTable( liquibaseDatabase );
        this.testDatabase.createUnitConversionTable( liquibaseDatabase );
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
     * Creates an instance of a {@link EnsembleRetrieverFactory} to test.
     * @throws SQLException if the factory could not be created
     */

    private void createEnsembleRetrieverFactory() throws SQLException
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
                                                      null );

        // Same right and baseline
        DataSourceBaselineConfig rightAndBaseline =
                new DataSourceBaselineConfig( DatasourceType.fromValue( "ensemble forecasts" ),
                                              sourceList,
                                              new Variable( STREAMFLOW, null ),
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

        FeatureTuple featureTuple = new FeatureTuple( FAKE_FEATURE, FAKE_FEATURE, FAKE_FEATURE );
        Set<FeatureTuple> allFeatures = Set.of( featureTuple );
        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( PROJECT_ID );
        Mockito.when( project.getFeatures() ).thenReturn( allFeatures );
        Mockito.when( project.getLeftVariableName() ).thenReturn( STREAMFLOW );
        Mockito.when( project.getRightVariableName() ).thenReturn( STREAMFLOW );
        Mockito.when( project.getBaselineVariableName() ).thenReturn( STREAMFLOW );
        Mockito.when( project.hasBaseline() ).thenReturn( true );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );

        // Create the factory instance
        UnitMapper unitMapper = UnitMapper.of( this.wresDatabase, CFS );
        this.factoryToTest = EnsembleRetrieverFactory.of( this.wresDatabase, featuresCache, project, featureTuple, unitMapper );
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

        Integer sourceId = sourceDetails.getId();

        assertNotNull( sourceId );

        // Add a project 
        Project project =
                new Project( this.mockSystemSettings,
                             this.wresDatabase,
                             this.mockExecutor,
                             new ProjectConfig( null, null, null, null, null, "test_project" ), PROJECT_ID );
        project.save();

        assertTrue( project.performedInsert() );

        assertEquals( PROJECT_ID, project.getId() );

        // Add a project source
        // There is no wres abstraction to help with this
        String projectSourceInsert = INSERT_INTO_WRES_PROJECT_SOURCE;

        // Project source for RIGHT
        projectSourceInsert = MessageFormat.format( projectSourceInsert,
                                                    PROJECT_ID,
                                                    sourceId,
                                                    LeftOrRightOrBaseline.RIGHT.value() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Project source for BASELINE - same source for simplicity
        String projectSourceBaselineInsert = INSERT_INTO_WRES_PROJECT_SOURCE;

        projectSourceBaselineInsert = MessageFormat.format( projectSourceBaselineInsert,
                                                            PROJECT_ID,
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
        Integer measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        // Add first member
        EnsembleDetails members = new EnsembleDetails();
        String ensembleName = "ENS";
        int firstMemberLabel = 123;
        members.setEnsembleName( ensembleName );
        members.setEnsembleMemberIndex( firstMemberLabel );
        members.save( this.wresDatabase );
        Integer firstMemberId = members.getId();

        assertNotNull( firstMemberId );

        // Add second member
        int secondMemberLabel = 567;
        members.setEnsembleName( ensembleName );
        members.setEnsembleMemberIndex( secondMemberLabel );
        members.save( this.wresDatabase );
        Integer secondMemberId = members.getId();

        assertNotNull( secondMemberId );

        // Add third member
        int thirdMemberLabel = 456;
        members.setEnsembleName( ensembleName );
        members.setEnsembleMemberIndex( thirdMemberLabel );
        members.save( this.wresDatabase );
        Integer thirdMemberId = members.getId();

        assertNotNull( thirdMemberId );

        // Add each member in turn
        // There is an abstraction to help with this, namely wres.io.data.details.TimeSeries, but the resulting 
        // prepared statement fails on wres.TimeSeriesSource, seemingly on the datatype of the timeseries_id column, 
        // although H2 reported the expected type. See #56214-102        

        // Two reference times, PT17H apart
        Instant referenceTime = Instant.parse( T2023_04_01T00_00_00Z );

        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     firstMemberId,
                                                     measurementUnitId,
                                                     referenceTime,
                                                     sourceId,
                                                     STREAMFLOW,
                                                     feature.getId() );
        firstTraceRow.setTimeScale( timeScale );
        int firstTraceRowId = firstTraceRow.getTimeSeriesID();

        // Successfully added row
        assertTrue( firstTraceRowId > 0 );

        wres.io.data.details.TimeSeries secondTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     secondMemberId,
                                                     measurementUnitId,
                                                     referenceTime,
                                                     sourceId,
                                                     STREAMFLOW,
                                                     feature.getId() );
        secondTraceRow.setTimeScale( timeScale );
        int secondTraceRowId = secondTraceRow.getTimeSeriesID();

        assertTrue( secondTraceRowId > 0 );

        // Third member
        wres.io.data.details.TimeSeries thirdTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     thirdMemberId,
                                                     measurementUnitId,
                                                     referenceTime,
                                                     sourceId,
                                                     STREAMFLOW,
                                                     feature.getId() );
        thirdTraceRow.setTimeScale( timeScale );
        int thirdTraceRowId = thirdTraceRow.getTimeSeriesID();


        // Add the time-series values to wres.TimeSeriesValue       
        Duration seriesIncrement = Duration.ofHours( 1 );
        double valueStart = 23.0;
        double valueIncrement = 7.0;

        // Insert template
        // As above, this does not work as a prepared statement via DataScripter
        String forecastInsert =
                "INSERT INTO wres.TimeSeriesValue (timeseries_id, lead, series_value) VALUES ({0},{1},{2})";

        // Insert the ensemble members into the db
        double forecastValue = valueStart;
        Map<Integer, Instant> series = new TreeMap<>();
        series.put( firstTraceRowId, referenceTime );
        series.put( secondTraceRowId, referenceTime );
        series.put( thirdTraceRowId, referenceTime );

        // Iterate and add the series values
        for ( Map.Entry<Integer, Instant> nextSeries : series.entrySet() )
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

        Integer sourceId = sourceDetails.getId();

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
        Integer measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        EnsembleDetails ensemble = new EnsembleDetails();
        ensemble.setEnsembleName( "ENS" );
        ensemble.setEnsembleMemberIndex( 123 );
        ensemble.save( this.wresDatabase );
        Integer ensembleId = ensemble.getId();

        assertNotNull( ensembleId );

        FeatureDetails details = new FeatureDetails( FAKE_FEATURE );
        details.save( this.wresDatabase );
        int featureId = details.getId();
        Instant latestObsDatetime = Instant.parse( "2023-04-01T10:00:00Z" );
        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     latestObsDatetime,
                                                     sourceId,
                                                     STREAMFLOW,
                                                     featureId );
        firstTraceRow.setTimeScale( timeScale );
        int firstTraceRowId = firstTraceRow.getTimeSeriesID();


        // Add some observations
        // There is no wres abstraction to help with this
        Instant seriesStart = Instant.parse( T2023_04_01T00_00_00Z );
        Duration seriesIncrement = Duration.ofHours( 1 );
        double valueStart = 23.0;
        double valueIncrement = 7.0;

        // Insert template
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

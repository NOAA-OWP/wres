package wres.io.retrieval.datashop;

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
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import wres.system.DatabaseConnectionSupplier;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Features;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.VariableDetails;
import wres.io.project.Project;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedRetrieverFactory}.
 * @author james.brown@hydrosolved.com
 */

@RunWith( PowerMockRunner.class )
@PrepareForTest( { SystemSettings.class, ConfigHelper.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class SingleValuedRetrieverFactoryTest
{

    private TestDatabase testDatabase;
    private ComboPooledDataSource dataSource;
    private Connection rawConnection;
    private @Mock DatabaseConnectionSupplier mockConnectionSupplier;

    /**
     * A project_id for testing;
     */

    private static final Integer PROJECT_ID = 1;

    /**
     * A variablefeature_id for testing.
     */

    private Integer variableFeatureId;

    /**
     * The measurement units for testing.
     */

    private static final String CFS = "CFS";

    /**
     * The feature name.
     */

    private static final String FAKE_FEATURE = "FAKE";

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
    public void runBeforeEachTest() throws Exception
    {
        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "SingleValuedRetrieverFactoryTest" );
        this.dataSource = this.testDatabase.getNewComboPooledDataSource();

        // Create the connection and schema
        this.createTheConnectionAndSchema();

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
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever()
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeries =
                builder.addEvent( Event.of( Instant.parse( T2023_04_01T01_00_00Z ), 30.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T07_00_00Z ), 72.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 79.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T09:00:00Z" ), 86.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T10:00:00Z" ), 93.0 ) )
                       .setTimeScale( TimeScale.of() )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetLeftRetrieverWithTimeWindowReturnsOneTimeSeriesWithFiveEvents()
    {

        // The time window to select events
        TimeWindow timeWindow = TimeWindow.of( Instant.parse( T2023_04_01T02_00_00Z ),
                                               Instant.parse( T2023_04_01T07_00_00Z ) );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getLeftRetriever( timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeries =
                builder.addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T07_00_00Z ), 72.0 ) )
                       .setTimeScale( TimeScale.of() )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetRightRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindow timeWindow = TimeWindow.of( Instant.parse( "2023-03-31T11:00:00Z" ),
                                               Instant.parse( T2023_04_01T00_00_00Z ),
                                               Instant.parse( T2023_04_01T01_00_00Z ),
                                               Instant.parse( T2023_04_01T04_00_00Z ) );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getRightRetriever( timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeries =
                builder.addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
                       .addReferenceTime( Instant.parse( T2023_04_01T00_00_00Z ), ReferenceTimeType.UNKNOWN )
                       .setTimeScale( TimeScale.of() )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetBaselineRetrieverWithTimeWindowReturnsOneTimeSeriesWithThreeEvents()
    {

        // The time window to select events
        TimeWindow timeWindow = TimeWindow.of( Instant.parse( "2023-03-31T11:00:00Z" ),
                                               Instant.parse( T2023_04_01T00_00_00Z ),
                                               Instant.parse( T2023_04_01T01_00_00Z ),
                                               Instant.parse( T2023_04_01T04_00_00Z ) );

        // Get the actual left series
        List<TimeSeries<Double>> actualCollection = this.factoryToTest.getBaselineRetriever( timeWindow )
                                                                      .get()
                                                                      .collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeries =
                builder.addEvent( Event.of( Instant.parse( T2023_04_01T02_00_00Z ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T03_00_00Z ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( T2023_04_01T04_00_00Z ), 51.0 ) )
                       .addReferenceTime( Instant.parse( T2023_04_01T00_00_00Z ), ReferenceTimeType.UNKNOWN )
                       .setTimeScale( TimeScale.of() )
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
        this.dataSource = null;
    }

    /**
     * Does the basic set-up work to create a connection and schema.
     * 
     * @throws Exception if the set-up failed
     */

    private void createTheConnectionAndSchema() throws Exception
    {
        // Also mock a plain datasource (which works per test unlike c3p0)
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        Mockito.when( this.mockConnectionSupplier.get() ).thenReturn( this.rawConnection );

        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute raw connection where needed:
        PowerMockito.mockStatic( SystemSettings.class );
        PowerMockito.when( SystemSettings.class, "getRawDatabaseConnection" )
                    .thenReturn( this.rawConnection );

        PowerMockito.whenNew( DatabaseConnectionSupplier.class )
                    .withNoArguments()
                    .thenReturn( this.mockConnectionSupplier );

        // Substitute our H2 connection pool for both pools:
        PowerMockito.when( SystemSettings.class, "getConnectionPool" )
                    .thenReturn( this.dataSource );
        PowerMockito.when( SystemSettings.class, "getHighPriorityConnectionPool" )
                    .thenReturn( this.dataSource );
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
        this.testDatabase.createVariableTable( liquibaseDatabase );
        this.testDatabase.createFeatureTable( liquibaseDatabase );
        this.testDatabase.createVariableFeatureTable( liquibaseDatabase );
        this.testDatabase.createObservationTable( liquibaseDatabase );
        this.testDatabase.createEnsembleTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesSourceTable( liquibaseDatabase );
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
                                                 null );
        List<DataSourceConfig.Source> sourceList = new ArrayList<>();

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      sourceList,
                                                      new Variable( STREAMFLOW, null, null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        // Same right and baseline
        DataSourceConfig rightAndBaseline = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                                  sourceList,
                                                                  new Variable( STREAMFLOW, null, null ),
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      rightAndBaseline,
                                                                      rightAndBaseline );

        ProjectConfig projectConfig = new ProjectConfig( inputsConfig, pairsConfig, null, null, null, null );

        TimeScale desiredTimeScale = TimeScale.of();

        Feature feature =
                new Feature( null, null, null, null, null, FAKE_FEATURE, null, null, null, null, null, null, null );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( PROJECT_ID );
        Mockito.when( project.getDesiredTimeScale() ).thenReturn( desiredTimeScale );
        Mockito.when( project.getLeftVariableFeatureId( feature ) ).thenReturn( this.variableFeatureId );
        Mockito.when( project.getRightVariableFeatureId( feature ) ).thenReturn( this.variableFeatureId );
        Mockito.when( project.getBaselineVariableFeatureId( feature ) ).thenReturn( this.variableFeatureId );
        Mockito.when( project.hasBaseline() ).thenReturn( true );
        Mockito.when( project.usesProbabilityThresholds() ).thenReturn( false );

        // Create the factory instance
        UnitMapper unitMapper = UnitMapper.of( CFS );
        this.factoryToTest = SingleValuedRetrieverFactory.of( project, feature, unitMapper );
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

        sourceDetails.save();

        assertTrue( sourceDetails.performedInsert() );

        Integer sourceId = sourceDetails.getId();

        assertNotNull( sourceId );

        // Add a project 
        Project project =
                new Project( new ProjectConfig( null, null, null, null, null, "test_project" ), PROJECT_ID );
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

        DataScripter script = new DataScripter( projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Project source for BASELINE - same source for simplicity
        String projectSourceBaselineInsert = INSERT_INTO_WRES_PROJECT_SOURCE;

        projectSourceBaselineInsert = MessageFormat.format( projectSourceBaselineInsert,
                                                            PROJECT_ID,
                                                            sourceId,
                                                            LeftOrRightOrBaseline.BASELINE.value() );

        DataScripter scriptBaseline = new DataScripter( projectSourceBaselineInsert );
        int rowsBaseline = scriptBaseline.execute();

        assertEquals( 1, rowsBaseline );

        // Add a feature
        FeatureDetails feature = new FeatureDetails();
        feature.setLid( FAKE_FEATURE );
        feature.save();

        assertNotNull( feature.getId() );

        // Add a variable
        VariableDetails variable = new VariableDetails();
        variable.setVariableName( STREAMFLOW );
        variable.save();

        assertNotNull( variable.getId() );

        // Get (and add) a variablefeature
        // There is no wres abstraction to help with this, but there is a static helper
        this.variableFeatureId = Features.getVariableFeatureByFeature( feature, variable.getId() );

        assertNotNull( this.variableFeatureId );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( CFS );
        measurement.save();
        Integer measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        EnsembleDetails ensemble = new EnsembleDetails();
        ensemble.setEnsembleName( "ENS" );
        ensemble.setEnsembleMemberIndex( 123 );
        ensemble.save();
        Integer ensembleId = ensemble.getId();

        assertNotNull( ensembleId );

        // Add two forecasts
        // There is an abstraction to help with this, namely wres.io.data.details.TimeSeries, but the resulting 
        // prepared statement fails on wres.TimeSeriesSource, seemingly on the datatype of the timeseries_id column, 
        // although H2 reported the expected type. See #56214-102        

        // Two reference times, PT17H apart
        Instant firstReference = Instant.parse( T2023_04_01T00_00_00Z );
        Instant secondReference = Instant.parse( T2023_04_01T17_00_00Z );

        TimeScale timeScale = TimeScale.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        String timeSeriesInsert = "INSERT INTO wres.TimeSeries (variablefeature_id,"
                                  + "ensemble_id,"
                                  + "measurementunit_id,"
                                  + "initialization_date,"
                                  + "scale_period,"
                                  + "scale_function) "
                                  + "VALUES (?,"
                                  + "?,"
                                  + "?,"
                                  + "(?)::timestamp without time zone,"
                                  + "?,"
                                  + "(?)::scale_function )";

        DataScripter seriesOneScript = new DataScripter( timeSeriesInsert );

        int rowAdded = seriesOneScript.execute( this.variableFeatureId,
                                                ensembleId,
                                                measurementUnitId,
                                                firstReference.toString(),
                                                timeScale.getPeriod().toMinutesPart(),
                                                timeScale.getFunction().name() );

        // One row added
        assertEquals( 1, rowAdded );

        assertNotNull( seriesOneScript.getInsertedIds() );
        assertEquals( 1, seriesOneScript.getInsertedIds().size() );

        Integer firstSeriesId = seriesOneScript.getInsertedIds().get( 0 ).intValue();

        // See above and #56214-102. This statement fails in wres.io.data.details.TimeSeries as a prepared statement
        // via DataScripter
        String timeSeriesSourceInsert = "INSERT INTO wres.TimeSeriesSource (timeseries_id,source_id)"
                                        + "VALUES ({0},{1})";

        String seriesOneSource = MessageFormat.format( timeSeriesSourceInsert,
                                                       firstSeriesId,
                                                       sourceId );

        DataScripter seriesOneSourceScript = new DataScripter( seriesOneSource );

        int anotherRowAdded = seriesOneSourceScript.execute();

        // One row added
        assertEquals( 1, anotherRowAdded );

        // Add the second series
        DataScripter seriesTwoScript = new DataScripter( timeSeriesInsert );

        int rowAddedTwo = seriesTwoScript.execute( this.variableFeatureId,
                                                   ensembleId,
                                                   measurementUnitId,
                                                   secondReference.toString(),
                                                   timeScale.getPeriod().toMinutesPart(),
                                                   timeScale.getFunction().name() );

        // One row added
        assertEquals( 1, rowAddedTwo );

        assertNotNull( seriesTwoScript.getInsertedIds() );
        assertEquals( 1, seriesTwoScript.getInsertedIds().size() );

        Integer secondSeriesId = seriesTwoScript.getInsertedIds().get( 0 ).intValue();

        String seriesTwoSource = MessageFormat.format( timeSeriesSourceInsert,
                                                       secondSeriesId,
                                                       sourceId );

        DataScripter seriesTwoSourceScript = new DataScripter( seriesTwoSource );

        int anotherRowAddedTwo = seriesTwoSourceScript.execute();

        // One row added
        assertEquals( 1, anotherRowAddedTwo );

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
        Map<Integer, Instant> series = new TreeMap<>();
        series.put( firstSeriesId, firstReference );
        series.put( secondSeriesId, secondReference );

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

                DataScripter forecastScript = new DataScripter( insert );

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

        sourceDetails.save();

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

        DataScripter script = new DataScripter( projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( CFS );
        measurement.save();
        Integer measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        // Add some observations
        // There is no wres abstraction to help with this      
        int scalePeriod = 1;
        String scaleFunction = TimeScaleFunction.UNKNOWN.name();

        Instant seriesStart = Instant.parse( T2023_04_01T00_00_00Z );
        Duration seriesIncrement = Duration.ofHours( 1 );
        double valueStart = 23.0;
        double valueIncrement = 7.0;

        // Insert template
        String observationInsert =
                "INSERT INTO wres.Observation"
                                   + "(variablefeature_id, observation_time, observed_value, measurementunit_id, "
                                   + "source_id, scale_period, scale_function) "
                                   + "VALUES ({0},''{1}'',{2},{3},{4},{5},''{6}'')";

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
                                                  this.variableFeatureId,
                                                  observationTime.toString(),
                                                  observedValue,
                                                  measurementUnitId,
                                                  sourceId,
                                                  scalePeriod,
                                                  scaleFunction );

            DataScripter observedScript = new DataScripter( insert );

            int row = observedScript.execute();

            // One row added
            assertEquals( 1, row );
        }

    }

}

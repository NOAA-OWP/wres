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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
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

import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.io.data.caching.Features;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.VariableDetails;
import wres.io.project.Project;
import wres.io.retrieval.AnalysisRetriever.DuplicatePolicy;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link AnalysisRetriever}.
 * @author james.brown@hydrosolved.com
 */

public class AnalysisRetrieverTest
{
    private static final Instant T2023_04_01T00_00_00Z = Instant.parse( "2023-04-01T00:00:00Z" );
    private static final Instant T2023_04_01T03_00_00Z = Instant.parse( "2023-04-01T03:00:00Z" );
    private static final Instant T2023_04_01T06_00_00Z = Instant.parse( "2023-04-01T06:00:00Z" );
    // Comparator for ordering time-series by reference time
    private final Comparator<TimeSeries<Double>> comparator =
            ( a, b ) -> a.getReferenceTimes()
                         .get( ReferenceTimeType.ANALYSIS_START_TIME )
                         .compareTo( b.getReferenceTimes()
                                      .get( ReferenceTimeType.ANALYSIS_START_TIME ) );
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
     * A variablefeature_id for testing.
     */

    private Integer variableFeatureId;

    /**
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.RIGHT;

    /**
     * The measurement units for testing.
     */

    private static final String UNITS = "CFS";

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
    public void setup() throws Exception
    {
        MockitoAnnotations.initMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "SingleValuedForecastRetrieverTest" );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create the connection and schema, set up mock system settings
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        this.featuresCache = new Features( this.wresDatabase );

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase();

        // Create the unit mapper
        this.unitMapper = UnitMapper.of( this.wresDatabase, UNITS );
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
                                               .setDatabase( this.wresDatabase ).setProjectId( PROJECT_ID )
                                               .setVariableFeatureId( this.variableFeatureId )
                                               .setUnitMapper( this.unitMapper )
                                               .setLeftOrRightOrBaseline( LRB )
                                               .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = analysisRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.collect( Collectors.toList() );

        actualCollection.sort( this.comparator );

        // There are three time-series, so assert that
        assertEquals( 3, actualCollection.size() );
        TimeSeries<Double> actualSeriesOne = actualCollection.get( 0 );
        TimeSeries<Double> actualSeriesTwo = actualCollection.get( 1 );
        TimeSeries<Double> actualSeriesThree = actualCollection.get( 2 );

        // Create the first expected series
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesOne =
                new TimeSeriesBuilder<Double>().setMetadata( metadata )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeriesBuilder<Double>().setMetadata( metadataTwo )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 72.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 79.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 86.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );

        // Create the third expected series
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesThree =
                new TimeSeriesBuilder<Double>().setMetadata( metadataThree )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 114.0 ) )
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
                                               .setProjectId( PROJECT_ID )
                                               .setVariableFeatureId( this.variableFeatureId )
                                               .setUnitMapper( this.unitMapper )
                                               .setLeftOrRightOrBaseline( LRB )
                                               .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = analysisRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.collect( Collectors.toList() );

        actualCollection.sort( this.comparator );

        // There are three time-series, so assert that
        assertEquals( 3, actualCollection.size() );
        TimeSeries<Double> actualSeriesOne = actualCollection.get( 0 );
        TimeSeries<Double> actualSeriesTwo = actualCollection.get( 1 );
        TimeSeries<Double> actualSeriesThree = actualCollection.get( 2 );

        // Create the first expected series
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesOne =
                new TimeSeriesBuilder<Double>().setMetadata( metadata )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeriesBuilder<Double>().setMetadata( metadataTwo )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 72.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );

        // Create the third expected series
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesThree =
                new TimeSeriesBuilder<Double>().setMetadata( metadataThree )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 114.0 ) )
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
                                               .setProjectId( PROJECT_ID )
                                               .setVariableFeatureId( this.variableFeatureId )
                                               .setUnitMapper( this.unitMapper )
                                               .setLeftOrRightOrBaseline( LRB )
                                               .build();

        // Get the time-series
        Stream<TimeSeries<Double>> forecastSeries = analysisRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = forecastSeries.collect( Collectors.toList() );

        actualCollection.sort( this.comparator );

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
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesOne =
                new TimeSeriesBuilder<Double>().setMetadata( metadata )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata metadataTwo =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T00_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeriesBuilder<Double>().setMetadata( metadataTwo )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );

        // Create the third expected series
        TimeSeriesMetadata metadataThree =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesThree =
                new TimeSeriesBuilder<Double>().setMetadata( metadataThree )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 72.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesThree, actualSeriesThree );

        // Create the fourth expected series
        TimeSeriesMetadata metadataFour =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T03_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesFour =
                new TimeSeriesBuilder<Double>().setMetadata( metadataFour )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 79.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesFour, actualSeriesFour );

        // Create the fifth expected series
        TimeSeriesMetadata metadataFive =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesFive =
                new TimeSeriesBuilder<Double>().setMetadata( metadataFive )
                                               .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 114.0 ) )
                                               .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesFive, actualSeriesFive );

        // Create the sixth expected series
        TimeSeriesMetadata metadataSix =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                               T2023_04_01T06_00_00Z ),
                                       TimeScale.of(),
                                       Integer.toString( this.variableFeatureId ),
                                       Integer.toString( this.variableFeatureId ),
                                       "CFS" );
        TimeSeries<Double> expectedSeriesSix =
                new TimeSeriesBuilder<Double>().setMetadata( metadataSix )
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
     * Performs the detailed set-up work to add three time-series to the database. Some assertions are made here, which
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
        String projectSourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ({0},{1},''{2}'')";

        //Format 
        projectSourceInsert = MessageFormat.format( projectSourceInsert,
                                                    PROJECT_ID,
                                                    sourceId,
                                                    LRB.value() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Add a feature
        FeatureDetails feature = new FeatureDetails();
        feature.setLid( "FEAT" );
        feature.save( this.wresDatabase );

        assertNotNull( feature.getId() );

        // Add a variable
        VariableDetails variable = new VariableDetails();
        variable.setVariableName( "VAR" );
        variable.save( this.wresDatabase );

        assertNotNull( variable.getId() );

        // Get (and add) a variablefeature
        // There is no wres abstraction to help with this, but there is a static helper
        this.variableFeatureId = this.featuresCache.getVariableFeatureByFeature( feature, variable.getId() );

        assertNotNull( this.variableFeatureId );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( UNITS );
        measurement.save( this.wresDatabase );
        Integer measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        EnsembleDetails ensemble = new EnsembleDetails();
        ensemble.setEnsembleName( "ENS" );
        ensemble.setEnsembleMemberIndex( 123 );
        ensemble.save( this.wresDatabase );
        Integer ensembleId = ensemble.getId();

        assertNotNull( ensembleId );

        // Add two three analysis time-series
        // There is an abstraction to help with this, namely wres.io.data.details.TimeSeries, but the resulting 
        // prepared statement fails on wres.TimeSeriesSource, seemingly on the datatype of the timeseries_id column, 
        // although H2 reported the expected type. See #56214-102

        // Three reference times, PT3H apart
        Instant firstReference = T2023_04_01T00_00_00Z;
        Instant secondReference = T2023_04_01T03_00_00Z;
        Instant thirdReference = T2023_04_01T06_00_00Z;

        TimeScale timeScale = TimeScale.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        String timeSeriesInsert = "INSERT INTO wres.TimeSeries (variablefeature_id,"
                                  + "ensemble_id,"
                                  + "measurementunit_id,"
                                  + "initialization_date,"
                                  + "scale_period,"
                                  + "scale_function,"
                                  + "source_id ) "
                                  + "VALUES (?,"
                                  + "?,"
                                  + "?,"
                                  + "(?)::timestamp without time zone,"
                                  + "?,"
                                  + "?,"
                                  + "? )";

        DataScripter seriesOneScript = new DataScripter( this.wresDatabase,
                                                         timeSeriesInsert );

        int rowAdded = seriesOneScript.execute( this.variableFeatureId,
                                                ensembleId,
                                                measurementUnitId,
                                                firstReference.toString(),
                                                timeScale.getPeriod().toMinutesPart(),
                                                timeScale.getFunction().name(),
                                                sourceId );

        // One row added
        assertEquals( 1, rowAdded );

        assertNotNull( seriesOneScript.getInsertedIds() );
        assertEquals( 1, seriesOneScript.getInsertedIds().size() );

        Integer firstSeriesId = seriesOneScript.getInsertedIds().get( 0 ).intValue();

        // Add the second series
        DataScripter seriesTwoScript = new DataScripter( this.wresDatabase,
                                                         timeSeriesInsert );

        int rowAddedTwo = seriesTwoScript.execute( this.variableFeatureId,
                                                   ensembleId,
                                                   measurementUnitId,
                                                   secondReference.toString(),
                                                   timeScale.getPeriod().toMinutesPart(),
                                                   timeScale.getFunction().name(),
                                                   sourceId );

        // One row added
        assertEquals( 1, rowAddedTwo );

        assertNotNull( seriesTwoScript.getInsertedIds() );
        assertEquals( 1, seriesTwoScript.getInsertedIds().size() );

        Integer secondSeriesId = seriesTwoScript.getInsertedIds().get( 0 ).intValue();


        // Add the third series
        DataScripter seriesThreeScript = new DataScripter( this.wresDatabase,
                                                           timeSeriesInsert );

        int rowAddedThree = seriesThreeScript.execute( this.variableFeatureId,
                                                       ensembleId,
                                                       measurementUnitId,
                                                       thirdReference.toString(),
                                                       timeScale.getPeriod().toMinutesPart(),
                                                       timeScale.getFunction().name(),
                                                       sourceId );

        // One row added
        assertEquals( 1, rowAddedThree );

        assertNotNull( seriesThreeScript.getInsertedIds() );
        assertEquals( 1, seriesThreeScript.getInsertedIds().size() );

        Integer thirdSeriesId = seriesThreeScript.getInsertedIds().get( 0 ).intValue();

        // Add the time-series values to wres.TimeSeriesValue       
        Duration seriesIncrement = Duration.ofHours( 1 );
        double valueStart = 23.0;
        double valueIncrement = 7.0;

        // Insert template
        // As above, this does not work as a prepared statement via DataScripter
        String forecastInsert =
                "INSERT INTO wres.TimeSeriesValue (timeseries_id, lead, series_value) VALUES ({0},{1},{2})";

        // Insert the time-series values into the db
        double analysisValue = valueStart;
        Map<Integer, Instant> series = new TreeMap<>();
        series.put( firstSeriesId, firstReference );
        series.put( secondSeriesId, secondReference );
        series.put( thirdSeriesId, thirdReference );

        // Iterate and add the series values
        for ( Map.Entry<Integer, Instant> nextSeries : series.entrySet() )
        {
            Instant validTime = nextSeries.getValue();

            for ( long i = 0; i < 6; i++ )
            {
                // Increment the valid datetime and value
                validTime = validTime.plus( seriesIncrement );
                analysisValue = analysisValue + valueIncrement;
                int lead = (int) seriesIncrement.multipliedBy( i + 1 ).toMinutes();

                // Insert
                String insert = MessageFormat.format( forecastInsert,
                                                      nextSeries.getKey(),
                                                      lead,
                                                      analysisValue );

                DataScripter forecastScript = new DataScripter( this.wresDatabase,
                                                                insert );

                int row = forecastScript.execute();

                // One row added
                assertEquals( 1, row );
            }
        }

    }

}

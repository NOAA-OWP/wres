package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import static wres.io.retrieval.RetrieverTestConstants.*;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
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

import wres.datamodel.FeatureKey;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
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
 * Tests the {@link ObservationRetriever}.
 * @author james.brown@hydrosolved.com
 */

public class ObservationRetrieverTest
{
    private static final String SECOND_TIME = "2023-04-01T09:00:00Z";
    private static final String FIRST_TIME = "2023-04-01T03:00:00Z";

    private static final FeatureKey FEATURE = FeatureKey.of( "FEAT" );
    private static final String VARIABLE_NAME = "VAR";

    @Mock
    private SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;
    @Mock
    private Executor mockExecutor;
    private Features featuresCache;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;


    /**
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.LEFT;

    /**
     * The measurement units for testing.
     */

    private static final String UNITS = "CFS";

    /**
     * Unit mapper.
     */

    private UnitMapper unitMapper;

    /**
     * Error message when attempting to retrieve by identifier.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of observed time-series by identifier is not "
                                                      + "currently possible.";

    @BeforeClass
    public static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void setup() throws Exception
    {
        MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "ObservationRetrieverTest" );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

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

        // Create the connection and schema
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addAnObservedTimeSeriesWithTenEventsToTheDatabase();

        this.unitMapper = UnitMapper.of( this.wresDatabase, UNITS );
    }

    @Test
    public void testRetrievalOfObservedTimeSeriesWithTenEvents()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> observedRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setFeaturesCache( this.featuresCache )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeature( FEATURE )
                                                  .setUnitMapper( this.unitMapper )
                                                  .setLeftOrRightOrBaseline( LRB )
                                                  .build();

        // Get the time-series
        Stream<TimeSeries<Double>> observedSeries = observedRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = observedSeries.collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap(),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       "CFS" );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                       .addEvent( Event.of( Instant.parse( FIRST_TIME ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 72.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 79.0 ) )
                       .addEvent( Event.of( Instant.parse( SECOND_TIME ), 86.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T10:00:00Z" ), 93.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testRetrievalOfPoolShapedObservedTimeSeriesWithSevenEvents()
    {
        // Build the pool boundaries
        TimeWindowOuter poolBoundaries =
                TimeWindowOuter.of( Instant.parse( "2023-04-01T02:00:00Z" ), Instant.parse( SECOND_TIME ) );

        // Build the retriever
        Retriever<TimeSeries<Double>> observedRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setFeaturesCache( this.featuresCache )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeature( FEATURE )
                                                  .setUnitMapper( this.unitMapper )
                                                  .setTimeWindow( poolBoundaries )
                                                  .setLeftOrRightOrBaseline( LRB )
                                                  .build();

        // Get the time-series
        Stream<TimeSeries<Double>> observedSeries = observedRetriever.get();

        // Stream into a collection
        List<TimeSeries<Double>> actualCollection = observedSeries.collect( Collectors.toList() );

        // There is only one time-series, so assert that
        assertEquals( 1, actualCollection.size() );
        TimeSeries<Double> actualSeries = actualCollection.get( 0 );

        // Assert correct number of events
        assertEquals( 7, actualSeries.getEvents().size() );

        // Create the expected series
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap(),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       "CFS" );
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        TimeSeries<Double> expectedSeries =
                builder.setMetadata( expectedMetadata )
                       .addEvent( Event.of( Instant.parse( FIRST_TIME ), 44.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T06:00:00Z" ), 65.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T07:00:00Z" ), 72.0 ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T08:00:00Z" ), 79.0 ) )
                       .addEvent( Event.of( Instant.parse( SECOND_TIME ), 86.0 ) )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetAllIdentifiersThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setFeaturesCache( this.featuresCache )
                                                  .setUnitMapper( this.unitMapper )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeature( FEATURE )
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
        Retriever<TimeSeries<Double>> forecastRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeature( FEATURE )
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
        Retriever<TimeSeries<Double>> forecastRetriever =
                new ObservationRetriever.Builder().setDatabase( this.wresDatabase )
                                                  .setProjectId( PROJECT_ID )
                                                  .setVariableName( VARIABLE_NAME )
                                                  .setFeature( FEATURE )
                                                  .setUnitMapper( this.unitMapper )
                                                  .setLeftOrRightOrBaseline( LRB )
                                                  .build();

        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               () -> forecastRetriever.get( LongStream.of() ) );

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

        this.testDatabase.createSourceTable( liquibaseDatabase );
        this.testDatabase.createProjectTable( liquibaseDatabase );
        this.testDatabase.createProjectSourceTable( liquibaseDatabase );
        this.testDatabase.createFeatureTable( liquibaseDatabase );
        this.testDatabase.createEnsembleTable( liquibaseDatabase );
        this.testDatabase.createMeasurementUnitTable( liquibaseDatabase );
        this.testDatabase.createUnitConversionTable( liquibaseDatabase );
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
        String projectSourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ({0},{1},''{2}'')";

        //Format 
        projectSourceInsert = MessageFormat.format( projectSourceInsert,
                                                    project.getId(),
                                                    sourceId,
                                                    LRB.value() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Add a feature
        FeatureDetails feature = new FeatureDetails( FEATURE );
        feature.save( this.wresDatabase );

        assertNotNull( feature.getId() );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( UNITS );
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
                                                     1,
                                                     measurementUnitId,
                                                     latestObsDatetime,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     feature.getId() );
        firstTraceRow.setTimeScale( timeScale );
        long firstTraceRowId = firstTraceRow.getTimeSeriesID();


        // Add some observations

        Instant seriesStart = Instant.parse( "2023-04-01T00:00:00Z" );
        Duration seriesIncrement = Duration.ofHours( 1 );
        double valueStart = 23.0;
        double valueIncrement = 7.0;

        // Insert template
        // As above, this does not work as a prepared statement via DataScripter
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

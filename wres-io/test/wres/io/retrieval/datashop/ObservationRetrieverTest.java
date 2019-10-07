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
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeWindow;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.VariableDetails;
import wres.io.project.Project;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link ObservationRetriever}.
 * @author james.brown@hydrosolved.com
 */

@RunWith( PowerMockRunner.class )
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class ObservationRetrieverTest
{
    private static final String SECOND_TIME = "2023-04-01T09:00:00Z";
    private static final String FIRST_TIME = "2023-04-01T03:00:00Z";
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
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.LEFT;

    /**
     * The measurement units for testing.
     */

    private static final String UNITS = "CFS";

    @BeforeClass
    public static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @Before
    public void setup() throws Exception
    {
        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "ObservationRetrieverTest" );
        this.dataSource = this.testDatabase.getNewComboPooledDataSource();
        
        // Create the connection and schema
        this.createTheConnectionAndSchema();
        
        // Create the tables
        this.addTheDatabaseAndTables();
        
        // Add some data for testing
        this.addAnObservedTimeSeriesWithTenEventsToTheDatabase();
    }

    @Test
    public void testRetrievalOfObservedTimeSeriesWithTenEvents()
    {
        // Desired units are the same as the existing units
        UnitMapper mapper = UnitMapper.of( UNITS );

        // Build the retriever
        TimeSeriesRetriever<Double> observedRetriever =
                new ObservationRetriever.Builder().setProjectId( PROJECT_ID )
                                                  .setVariableFeatureId( this.variableFeatureId )
                                                  .setUnitMapper( mapper )
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
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeries =
                builder.addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
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
        // Desired units are the same as the existing units
        UnitMapper mapper = UnitMapper.of( UNITS );

        // Build the pool boundaries
        TimeWindow poolBoundaries =
                TimeWindow.of( Instant.parse( "2023-04-01T02:00:00Z" ), Instant.parse( SECOND_TIME ) );
        
        // Build the retriever
        TimeSeriesRetriever<Double> observedRetriever =
                new ObservationRetriever.Builder().setProjectId( PROJECT_ID )
                                                  .setVariableFeatureId( this.variableFeatureId )
                                                  .setUnitMapper( mapper )
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
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeries =
                builder.addEvent( Event.of( Instant.parse( FIRST_TIME ), 44.0 ) )
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

    @After
    public void tearDown() throws SQLException
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

        this.testDatabase.createSourceTable( liquibaseDatabase );
        this.testDatabase.createProjectTable( liquibaseDatabase );
        this.testDatabase.createProjectSourceTable( liquibaseDatabase );
        this.testDatabase.createVariableTable( liquibaseDatabase );
        this.testDatabase.createFeatureTable( liquibaseDatabase );
        this.testDatabase.createVariableFeatureTable( liquibaseDatabase );
        this.testDatabase.createObservationTable( liquibaseDatabase );
        this.testDatabase.createMeasurementUnitTable( liquibaseDatabase );
        this.testDatabase.createUnitConversionTable( liquibaseDatabase );
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
        String projectSourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ({0},{1},''{2}'')";

        //Format 
        projectSourceInsert = MessageFormat.format( projectSourceInsert,
                                                    PROJECT_ID,
                                                    sourceId,
                                                    LRB.value() );

        DataScripter script = new DataScripter( projectSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        // Add a feature
        FeatureDetails feature = new FeatureDetails();
        feature.setLid( "FEAT" );
        feature.save();

        assertNotNull( feature.getId() );

        // Add a variable
        VariableDetails variable = new VariableDetails();
        variable.setVariableName( "VAR" );
        variable.save();

        assertNotNull( variable.getId() );

        // Get (and add) a variablefeature
        // There is no wres abstraction to help with this, but there is a static helper
        this.variableFeatureId = Features.getVariableFeatureByFeature( feature, variable.getId() );

        assertNotNull( this.variableFeatureId );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( UNITS );
        measurement.save();
        Integer measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        // Add some observations
        // There is no wres abstraction to help with this      
        int scalePeriod = 1;
        String scaleFunction = TimeScaleFunction.UNKNOWN.name();

        Instant seriesStart = Instant.parse( "2023-04-01T00:00:00Z" );
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

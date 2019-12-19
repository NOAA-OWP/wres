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
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeWindow;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Features;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.VariableDetails;
import wres.io.project.Project;
import wres.io.retrieval.Retriever;
import wres.io.retrieval.SingleValuedForecastRetriever;
import wres.io.retrieval.UnitMapper;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedForecastRetriever}.
 * @author james.brown@hydrosolved.com
 */

@RunWith( PowerMockRunner.class )
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class SingleValuedForecastRetrieverTest
{
    private static final String T2023_04_01T19_00_00Z = "2023-04-01T19:00:00Z";
    private static final String T2023_04_01T17_00_00Z = "2023-04-01T17:00:00Z";
    private static final String T2023_04_01T00_00_00Z = "2023-04-01T00:00:00Z";
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
        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "SingleValuedForecastRetrieverTest" );
        this.dataSource = this.testDatabase.getNewComboPooledDataSource();

        // Create the connection and schema
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addTwoForecastTimeSeriesEachWithFiveEventsToTheDatabase();

        // Create the unit mapper
        this.unitMapper = UnitMapper.of( UNITS );
    }

    @Test
    public void testRetrievalOfTwoForecastTimeSeriesEachWithFiveEvents()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                           .setVariableFeatureId( this.variableFeatureId )
                                                           .setUnitMapper( this.unitMapper )
                                                           .setLeftOrRightOrBaseline( LRB )
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
        TimeSeriesBuilder<Double> builderOne = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeriesOne =
                builderOne.addReferenceTime( Instant.parse( T2023_04_01T00_00_00Z ), ReferenceTimeType.UNKNOWN )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ), 30.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ), 37.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ), 44.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ), 58.0 ) )
                          .setTimeScale( TimeScale.of() )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesBuilder<Double> builderTwo = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeriesTwo =
                builderTwo.addReferenceTime( Instant.parse( T2023_04_01T17_00_00Z ), ReferenceTimeType.UNKNOWN )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T18:00:00Z" ), 65.0 ) )
                          .addEvent( Event.of( Instant.parse( T2023_04_01T19_00_00Z ), 72.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T20:00:00Z" ), 79.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T21:00:00Z" ), 86.0 ) )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T22:00:00Z" ), 93.0 ) )
                          .setTimeScale( TimeScale.of() )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );
    }   

    @Test
    public void testRetrievalOfTwoForecastTimeSeriesWithinTimeWindow()
    {
        // Set the time window filter, aka pool boundaries
        Instant referenceStart = Instant.parse( "2023-03-31T23:00:00Z" );
        Instant referenceEnd = Instant.parse( T2023_04_01T19_00_00Z );
        Instant validStart = Instant.parse( "2023-04-01T03:00:00Z" );
        Instant validEnd = Instant.parse( T2023_04_01T19_00_00Z );
        Duration leadStart = Duration.ofHours( 1 );
        Duration leadEnd = Duration.ofHours( 4 );

        TimeWindow timeWindow = TimeWindow.of( referenceStart, referenceEnd, validStart, validEnd, leadStart, leadEnd );

        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                           .setVariableFeatureId( this.variableFeatureId )
                                                           .setUnitMapper( this.unitMapper )
                                                           .setTimeWindow( timeWindow )
                                                           .setLeftOrRightOrBaseline( LRB )
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
        TimeSeriesBuilder<Double> builderOne = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeriesOne =
                builderOne.addReferenceTime( Instant.parse( T2023_04_01T00_00_00Z ), ReferenceTimeType.UNKNOWN )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                          .setTimeScale( TimeScale.of() )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesBuilder<Double> builderTwo = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeriesTwo =
                builderTwo.addReferenceTime( Instant.parse( T2023_04_01T17_00_00Z ), ReferenceTimeType.UNKNOWN )
                          .addEvent( Event.of( Instant.parse( T2023_04_01T19_00_00Z ), 72.0 ) )
                          .setTimeScale( TimeScale.of() )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesTwo, actualSeriesTwo );
    }

    @Test
    public void testGetRetrievalOfTimeSeriesIdentifiersReturnsTwoIdentifiers()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                           .setVariableFeatureId( this.variableFeatureId )
                                                           .setUnitMapper( this.unitMapper )
                                                           .setLeftOrRightOrBaseline( LRB )
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
                new SingleValuedForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                           .setVariableFeatureId( this.variableFeatureId )
                                                           .setUnitMapper( this.unitMapper )
                                                           .setLeftOrRightOrBaseline( LRB )
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

}

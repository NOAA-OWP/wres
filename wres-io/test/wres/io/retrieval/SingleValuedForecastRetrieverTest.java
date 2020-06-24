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
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeWindow;
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
 * Tests the {@link SingleValuedForecastRetriever}.
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedForecastRetrieverTest
{
    private static final String T2023_04_01T19_00_00Z = "2023-04-01T19:00:00Z";
    private static final String T2023_04_01T17_00_00Z = "2023-04-01T17:00:00Z";
    private static final String T2023_04_01T00_00_00Z = "2023-04-01T00:00:00Z";
    private static final String VARIABLE_NAME = "V";
    private static final FeatureKey FEATURE = FeatureKey.of( "F" );
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
    public void setup() throws SQLException, LiquibaseException
    {
        MockitoAnnotations.initMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "SingleValuedForecastRetrieverTest" );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create the connection and schema
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
    public void testRetrievalOfTwoForecastTimeSeriesEachWithFiveEvents()
    {
        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.featuresCache )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeature( FEATURE )
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
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
                                       TimeScale.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       "CFS" );
        TimeSeriesBuilder<Double> builderOne = new TimeSeriesBuilder<>();
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
                                               Instant.parse( T2023_04_01T17_00_00Z ) ),
                                       TimeScale.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       "CFS" );
        TimeSeriesBuilder<Double> builderTwo = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeriesTwo =
                builderTwo.setMetadata( expectedMetadataTwo )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T18:00:00Z" ), 65.0 ) )
                          .addEvent( Event.of( Instant.parse( T2023_04_01T19_00_00Z ), 72.0 ) )
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
        Instant referenceEnd = Instant.parse( T2023_04_01T19_00_00Z );
        Instant validStart = Instant.parse( "2023-04-01T03:00:00Z" );
        Instant validEnd = Instant.parse( T2023_04_01T19_00_00Z );
        Duration leadStart = Duration.ofHours( 1 );
        Duration leadEnd = Duration.ofHours( 4 );

        TimeWindow timeWindow = TimeWindow.of( referenceStart, referenceEnd, validStart, validEnd, leadStart, leadEnd );

        // Build the retriever
        Retriever<TimeSeries<Double>> forecastRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( this.wresDatabase )
                                                           .setFeaturesCache( this.featuresCache )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeature( FEATURE )
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
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
                                       TimeScale.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       "CFS" );
        TimeSeriesBuilder<Double> builderOne = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeriesOne =
                builderOne.setMetadata( expectedMetadata )
                          .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ), 51.0 ) )
                          .build();

        // Actual series equals expected series
        assertEquals( expectedSeriesOne, actualSeriesOne );

        // Create the second expected series
        TimeSeriesMetadata expectedMetadataTwo =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                               Instant.parse( T2023_04_01T17_00_00Z ) ),
                                       TimeScale.of(),
                                       VARIABLE_NAME,
                                       FEATURE,
                                       "CFS" );
        TimeSeriesBuilder<Double> builderTwo = new TimeSeriesBuilder<>();
        TimeSeries<Double> expectedSeriesTwo =
                builderTwo.setMetadata( expectedMetadataTwo )
                          .addEvent( Event.of( Instant.parse( T2023_04_01T19_00_00Z ), 72.0 ) )
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
                                                           .setFeaturesCache( this.featuresCache )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeature( FEATURE )
                                                           .setLeftOrRightOrBaseline( LRB )
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
                                                           .setFeaturesCache( this.featuresCache )
                                                           .setProjectId( PROJECT_ID )
                                                           .setVariableName( VARIABLE_NAME )
                                                           .setFeature( FEATURE )
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
     * Performs the detailed set-up work to add two time-series to the database. Some assertions are made here, which
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
        FeatureDetails feature = new FeatureDetails( FEATURE );
        feature.save( this.wresDatabase );

        assertNotNull( feature.getId() );

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

        // Add two forecasts
        // There is an abstraction to help with this, namely wres.io.data.details.TimeSeries, but the resulting 
        // prepared statement fails on wres.TimeSeriesSource, seemingly on the datatype of the timeseries_id column, 
        // although H2 reported the expected type. See #56214-102        

        // Two reference times, PT17H apart
        Instant firstReference = Instant.parse( T2023_04_01T00_00_00Z );
        Instant secondReference = Instant.parse( T2023_04_01T17_00_00Z );

        TimeScale timeScale = TimeScale.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     firstReference,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     feature.getId() );
        firstTraceRow.setTimeScale( timeScale );
        int firstTraceRowId = firstTraceRow.getTimeSeriesID();

        // Add the second series
        wres.io.data.details.TimeSeries secondTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     secondReference,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     feature.getId() );
        secondTraceRow.setTimeScale( timeScale );
        int secondTraceRowId = secondTraceRow.getTimeSeriesID();

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
        series.put( firstTraceRowId, firstReference );
        series.put( secondTraceRowId, secondReference );

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

}

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
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.Executor;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.io.data.caching.Ensembles;
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
 * Tests the {@link EnsembleForecastRetriever}.
 * @author james.brown@hydrosolved.com
 */

public class EnsembleForecastRetrieverTest
{
    private static final String T2023_04_01T00_00_00Z = "2023-04-01T00:00:00Z";
    @Mock
    private SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;
    @Mock
    private Executor mockExecutor;
    private Features featuresCache;
    private Ensembles ensemblesCache;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;

    /**
     * Identifier of the first ensemble member.
     */

    private Long firstMemberId;

    /**
     * Identifier of the second ensemble member.
     */

    private Long secondMemberId;

    /**
     * Identifier of the third ensemble member.
     */

    private Long thirdMemberId;

    /**
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.RIGHT;

    /**
     * The measurement units for testing.
     */

    private static final String UNITS = "CFS";

    private static final FeatureKey FEATURE = FeatureKey.of( "F" );
    private static final String VARIABLE_NAME = "Q";
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
        this.testDatabase = new TestDatabase( "EnsembleForecastRetrieverTest" );
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

        // Create a connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );

        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );

        // Create the tables
        this.addTheDatabaseAndTables();

        // Project depends on features cache. With ensemblesCache up here, NPE!
        this.featuresCache = new Features( this.wresDatabase );

        // Add some data for testing
        this.addOneForecastTimeSeriesWithFiveEventsAndThreeMembersToTheDatabase();

        // Create the unit mapper
        this.unitMapper = UnitMapper.of( this.wresDatabase, UNITS );

        // Create the orms
        this.ensemblesCache = new Ensembles( this.wresDatabase );
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
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
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
    {
        // Build the retriever with ensemble constraints
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setEnsemblesCache( this.ensemblesCache )
                                                       .setEnsembleIdsToInclude( Set.of( this.secondMemberId ) )
                                                       .setEnsembleIdsToExclude( Set.of( this.firstMemberId,
                                                                                         this.thirdMemberId ) )
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
                                               Instant.parse( T2023_04_01T00_00_00Z ) ),
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
     * Performs the detailed set-up work to add one time-series to the database. Some assertions are made here, which
     * could fail, in order to clarify the source of a failure.
     * 
     * @throws SQLException if the detailed set-up fails
     */

    private void addOneForecastTimeSeriesWithFiveEventsAndThreeMembersToTheDatabase() throws SQLException
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

        // Add first member
        EnsembleDetails members = new EnsembleDetails();
        //String ensembleName = "ENS";
        String firstMemberLabel = "123";
        members.setEnsembleName( firstMemberLabel );
        //members.setEnsembleMemberIndex( firstMemberLabel );
        members.save( this.wresDatabase );
        this.firstMemberId = members.getId();

        assertNotNull( this.firstMemberId );

        // Add second member
        String secondMemberLabel = "567";
        members.setEnsembleName( secondMemberLabel );
        //members.setEnsembleMemberIndex( secondMemberLabel );
        members.save( this.wresDatabase );
        this.secondMemberId = members.getId();

        assertNotNull( this.secondMemberId );

        // Add third member
        String thirdMemberLabel = "456";
        members.setEnsembleName( thirdMemberLabel );
        //members.setEnsembleMemberIndex( thirdMemberLabel );
        members.save( this.wresDatabase );
        this.thirdMemberId = members.getId();

        assertNotNull( this.thirdMemberId );

        // Add each member in turn
        // There is an abstraction to help with this, namely wres.io.data.details.TimeSeries, but the resulting 
        // prepared statement fails on wres.TimeSeriesSource, seemingly on the datatype of the timeseries_id column, 
        // although H2 reported the expected type. See #56214-102        

        // Two reference times, PT17H apart
        Instant referenceTime = Instant.parse( T2023_04_01T00_00_00Z );

        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     this.firstMemberId,
                                                     measurementUnitId,
                                                     referenceTime,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     feature.getId() );
        firstTraceRow.setTimeScale( timeScale );
        long firstTraceRowId = firstTraceRow.getTimeSeriesID();

        // Successfully added row
        assertTrue( firstTraceRowId > 0 );

        wres.io.data.details.TimeSeries secondTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     this.secondMemberId,
                                                     measurementUnitId,
                                                     referenceTime,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     feature.getId() );
        secondTraceRow.setTimeScale( timeScale );
        long secondTraceRowId = secondTraceRow.getTimeSeriesID();

        assertTrue( secondTraceRowId > 0 );

        // Third member
        wres.io.data.details.TimeSeries thirdTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     this.thirdMemberId,
                                                     measurementUnitId,
                                                     referenceTime,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     feature.getId() );
        thirdTraceRow.setTimeScale( timeScale );
        long thirdTraceRowId = thirdTraceRow.getTimeSeriesID();

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
        Map<Long, Instant> series = new TreeMap<>();
        series.put( firstTraceRowId, referenceTime );
        series.put( secondTraceRowId, referenceTime );
        series.put( thirdTraceRowId, referenceTime );

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

}

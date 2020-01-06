package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
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
import java.util.Set;
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
import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Features;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.VariableDetails;
import wres.io.project.Project;
import wres.io.retrieval.EnsembleForecastRetriever;
import wres.io.retrieval.Retriever;
import wres.io.retrieval.UnitMapper;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link EnsembleForecastRetriever}.
 * @author james.brown@hydrosolved.com
 */

@RunWith( PowerMockRunner.class )
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class EnsembleForecastRetrieverTest
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
     * Identifier of the first ensemble member.
     */

    private Integer firstMemberId;

    /**
     * Identifier of the second ensemble member.
     */

    private Integer secondMemberId;

    /**
     * Identifier of the third ensemble member.
     */

    private Integer thirdMemberId;

    /**
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.RIGHT;

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
    public void setup() throws Exception
    {
        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "EnsembleForecastRetrieverTest" );
        this.dataSource = this.testDatabase.getNewComboPooledDataSource();

        // Create the connection and schema
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addOneForecastTimeSeriesWithFiveEventsAndThreeMembersToTheDatabase();

        // Create the unit mapper
        unitMapper = UnitMapper.of( UNITS );
    }

    @Test
    public void testRetrievalOfOneTimeSeriesWithFiveEventsAndThreeMembers()
    {
        // Build the retriever
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                       .setVariableFeatureId( this.variableFeatureId )
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
        TimeSeriesBuilder<Ensemble> builder = new TimeSeriesBuilder<>();
        TimeSeries<Ensemble> expectedSeries =
                builder.addReferenceTime( Instant.parse( "2023-04-01T00:00:00Z" ), ReferenceTimeType.UNKNOWN )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ),
                                            Ensemble.of( 30.0, 100.0, 65.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ),
                                            Ensemble.of( 37.0, 107.0, 72.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ),
                                            Ensemble.of( 44.0, 114.0, 79.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ),
                                            Ensemble.of( 51.0, 121.0, 86.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ),
                                            Ensemble.of( 58.0, 128.0, 93.0 ) ) )
                       .setTimeScale( TimeScale.of() )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testRetrievalOfOneTimeSeriesWithFiveEventsAndOneMemberUsingEnsembleConstraints()
    {
        // Build the retriever with ensemble constraints
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setEnsembleIdsToInclude( Set.of( this.secondMemberId ) )
                                                       .setEnsembleIdsToExclude( Set.of( this.firstMemberId,
                                                                                         this.thirdMemberId ) )
                                                       .setProjectId( PROJECT_ID )
                                                       .setVariableFeatureId( this.variableFeatureId )
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
        TimeSeriesBuilder<Ensemble> builder = new TimeSeriesBuilder<>();
        TimeSeries<Ensemble> expectedSeries =
                builder.addReferenceTime( Instant.parse( "2023-04-01T00:00:00Z" ), ReferenceTimeType.UNKNOWN )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ),
                                            Ensemble.of( 65.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T02:00:00Z" ),
                                            Ensemble.of( 72.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T03:00:00Z" ),
                                            Ensemble.of( 79.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T04:00:00Z" ),
                                            Ensemble.of( 86.0 ) ) )
                       .addEvent( Event.of( Instant.parse( "2023-04-01T05:00:00Z" ),
                                            Ensemble.of( 93.0 ) ) )
                       .setTimeScale( TimeScale.of() )
                       .build();

        // Actual series equals expected series
        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    public void testGetAllIdentifiersThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                       .setVariableFeatureId( this.variableFeatureId )
                                                       .setUnitMapper( this.unitMapper )
                                                       .setLeftOrRightOrBaseline( LRB )
                                                       .build();

        UnsupportedOperationException expected = assertThrows( UnsupportedOperationException.class,
                                                               () -> forecastRetriever.getAllIdentifiers() );

        assertEquals( NO_IDENTIFIER_ERROR, expected.getMessage() );
    }

    @Test
    public void testGetByIdentifierThrowsExpectedException()
    {
        // Build the retriever
        Retriever<TimeSeries<Ensemble>> forecastRetriever =
                new EnsembleForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                       .setVariableFeatureId( this.variableFeatureId )
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
                new EnsembleForecastRetriever.Builder().setProjectId( PROJECT_ID )
                                                       .setVariableFeatureId( this.variableFeatureId )
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

        // Add first member
        EnsembleDetails members = new EnsembleDetails();
        String ensembleName = "ENS";
        int firstMemberLabel = 123;
        members.setEnsembleName( ensembleName );
        members.setEnsembleMemberIndex( firstMemberLabel );
        members.save();
        this.firstMemberId = members.getId();

        assertNotNull( this.firstMemberId );

        // Add second member
        int secondMemberLabel = 567;
        members.setEnsembleName( ensembleName );
        members.setEnsembleMemberIndex( secondMemberLabel );
        members.save();
        this.secondMemberId = members.getId();

        assertNotNull( this.secondMemberId );

        // Add third member
        int thirdMemberLabel = 456;
        members.setEnsembleName( ensembleName );
        members.setEnsembleMemberIndex( thirdMemberLabel );
        members.save();
        this.thirdMemberId = members.getId();

        assertNotNull( this.thirdMemberId );

        // Add each member in turn
        // There is an abstraction to help with this, namely wres.io.data.details.TimeSeries, but the resulting 
        // prepared statement fails on wres.TimeSeriesSource, seemingly on the datatype of the timeseries_id column, 
        // although H2 reported the expected type. See #56214-102        

        // Two reference times, PT17H apart
        Instant referenceTime = Instant.parse( "2023-04-01T00:00:00Z" );

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

        // First member
        DataScripter memberOneScript = new DataScripter( timeSeriesInsert );

        int rowAdded = memberOneScript.execute( this.variableFeatureId,
                                                this.firstMemberId,
                                                measurementUnitId,
                                                referenceTime.toString(),
                                                timeScale.getPeriod().toMinutesPart(),
                                                timeScale.getFunction().name(),
                                                sourceId );

        // One row added
        assertEquals( 1, rowAdded );

        assertNotNull( memberOneScript.getInsertedIds() );
        assertEquals( 1, memberOneScript.getInsertedIds().size() );

        Integer firstSeriesId = memberOneScript.getInsertedIds().get( 0 ).intValue();

        // Second member
        DataScripter memberTwoScript = new DataScripter( timeSeriesInsert );

        int rowAddedTwo = memberTwoScript.execute( this.variableFeatureId,
                                                   this.secondMemberId,
                                                   measurementUnitId,
                                                   referenceTime.toString(),
                                                   timeScale.getPeriod().toMinutesPart(),
                                                   timeScale.getFunction().name(),
                                                   sourceId );

        // One row added
        assertEquals( 1, rowAddedTwo );

        assertNotNull( memberTwoScript.getInsertedIds() );
        assertEquals( 1, memberTwoScript.getInsertedIds().size() );

        Integer secondSeriesId = memberTwoScript.getInsertedIds().get( 0 ).intValue();


        // Third member
        DataScripter memberThreeScript = new DataScripter( timeSeriesInsert );

        int rowAddedThree = memberThreeScript.execute( this.variableFeatureId,
                                                       this.thirdMemberId,
                                                       measurementUnitId,
                                                       referenceTime.toString(),
                                                       timeScale.getPeriod().toMinutesPart(),
                                                       timeScale.getFunction().name(),
                                                       sourceId );

        // One row added
        assertEquals( 1, rowAddedThree );

        assertNotNull( memberThreeScript.getInsertedIds() );
        assertEquals( 1, memberThreeScript.getInsertedIds().size() );

        Integer thirdSeriesId = memberThreeScript.getInsertedIds().get( 0 ).intValue();

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
        series.put( firstSeriesId, referenceTime );
        series.put( secondSeriesId, referenceTime );
        series.put( thirdSeriesId, referenceTime );

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

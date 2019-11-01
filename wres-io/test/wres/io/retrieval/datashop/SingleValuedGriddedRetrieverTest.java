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
import java.util.Map;
import java.util.TimeZone;

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
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import wres.system.DatabaseConnectionSupplier;
import wres.config.generated.CoordinateSelection;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;
import wres.grid.reading.GriddedReader;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.details.SourceDetails;
import wres.io.project.Project;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link SingleValuedGriddedRetriever}.
 * @author james.brown@hydrosolved.com
 */

@RunWith( PowerMockRunner.class )
@PrepareForTest( { SystemSettings.class, GriddedReader.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class SingleValuedGriddedRetrieverTest
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
     * A feature for testing.
     */

    private static final Feature FEATURE = new Feature( null,
                                                        new CoordinateSelection( 1F, 2F, 3F ),
                                                        null,
                                                        null,
                                                        null,
                                                        "FEAT",
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null );

    /**
     * A variable name for testing.
     */

    private static final String VARIABLE_NAME = "QINE";

    /**
     * A {@link LeftOrRightOrBaseline} for testing.
     */

    private static final LeftOrRightOrBaseline LRB = LeftOrRightOrBaseline.RIGHT;

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
        this.testDatabase = new TestDatabase( "SingleValuedGriddedRetriever" );
        this.dataSource = this.testDatabase.getNewComboPooledDataSource();

        // Create the connection and schema
        this.createTheConnectionAndSchema();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Add some data for testing
        this.addFiveGriddedSourcesToTheDatabase();
    }

    @Test
    public void testRetrievalOfTwoForecastTimeSeriesEachWithFiveEvents() throws Exception
    {
        // Desired units are the same as the existing units
        UnitMapper mapper = UnitMapper.of( UNITS );

        // Set the time window filter, aka pool boundaries to select a subset of sources

        // Remove the last source by reference time
        Instant referenceStart = Instant.parse( "2017-06-16T14:12:00Z" );
        Instant referenceEnd = Instant.parse( "2017-06-17T02:12:00Z" ); // PT1M before the last reference time

        // Remove the first source by valid time       
        Instant validStart = Instant.parse( "2017-06-16T15:13:00Z" ); // The first valid time, which is exclusive
        Instant validEnd = Instant.parse( "2017-06-17T07:13:00Z" ); // The last valid time, which is inclusive

        // Retain all sources by lead duration
        Duration leadStart = Duration.ofHours( 0 );
        Duration leadEnd = Duration.ofHours( 5 );

        TimeWindow timeWindow = TimeWindow.of( referenceStart, referenceEnd, validStart, validEnd, leadStart, leadEnd );

        // Build the retriever
        Retriever<TimeSeries<Double>> retriever =
                new SingleValuedGriddedRetriever.Builder().setVariableName( SingleValuedGriddedRetrieverTest.VARIABLE_NAME )
                                                          .setFeatures( List.of( SingleValuedGriddedRetrieverTest.FEATURE ) )
                                                          .setIsForecast( true )
                                                          .setProjectId( SingleValuedGriddedRetrieverTest.PROJECT_ID )
                                                          .setLeftOrRightOrBaseline( SingleValuedGriddedRetrieverTest.LRB )
                                                          .setUnitMapper( mapper )
                                                          .setTimeWindow( timeWindow )
                                                          .build();

        // Return a fake response from wres.grid, as only interested in the request here
        PowerMockito.mockStatic( GriddedReader.class );
        SingleValuedTimeSeriesResponse fakeResponse =
                SingleValuedTimeSeriesResponse.of( Map.of(),
                                                   SingleValuedGriddedRetrieverTest.VARIABLE_NAME,
                                                   SingleValuedGriddedRetrieverTest.UNITS );

        PowerMockito.when( GriddedReader.class, "getSingleValuedResponse", ArgumentMatchers.any( Request.class ) )
                    .thenReturn( fakeResponse );

        retriever.get();

        Request actual = Whitebox.getInternalState( retriever, "request" );

        // Create the expected request
        List<String> expectedPaths = List.of( "/this/is/just/a/test/source_2.nc",
                                              "/this/is/just/a/test/source_3.nc",
                                              "/this/is/just/a/test/source_4.nc" );
        Request expected = Fetcher.prepareRequest( expectedPaths,
                                                   List.of( SingleValuedGriddedRetrieverTest.FEATURE ),
                                                   SingleValuedGriddedRetrieverTest.VARIABLE_NAME,
                                                   timeWindow,
                                                   true,
                                                   null );

        // Verify the captured request
        assertEquals( expected, actual );
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

    private void addFiveGriddedSourcesToTheDatabase() throws SQLException
    {

        // Add a project 
        Project project =
                new Project( new ProjectConfig( null, null, null, null, null, "test_gridded_project" ),
                             SingleValuedGriddedRetrieverTest.PROJECT_ID );
        project.save();

        assertTrue( project.performedInsert() );

        assertEquals( SingleValuedGriddedRetrieverTest.PROJECT_ID, project.getId() );

        // Add a source for each of five forecast lead durations and output times
        // Also, add a project source for each one
        Instant sequenceOrigin = Instant.parse( "2017-06-16T11:13:00Z" );
        String projectSourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ("
                                     + SingleValuedGriddedRetrieverTest.PROJECT_ID
                                     + ",{0},''"
                                     + SingleValuedGriddedRetrieverTest.LRB.value()
                                     + "'')";
        for ( int i = 0; i < 5; i++ )
        {
            int nextLeadMinutes = ( i + 1 ) * 60;
            Instant nextTime = sequenceOrigin.plus( Duration.ofHours( 3 ).multipliedBy( ( i + 1 ) ) );

            SourceDetails.SourceKey sourceKey =
                    SourceDetails.createKey( URI.create( "/this/is/just/a/test/source_" + ( i + 1 ) + ".nc" ),
                                             nextTime.toString(),
                                             nextLeadMinutes,
                                             "abc12" + ( i + 3 ) );

            SourceDetails sourceDetails = new SourceDetails( sourceKey );
            sourceDetails.setIsPointData( false );
            sourceDetails.save();

            assertTrue( sourceDetails.performedInsert() );

            Integer sourceId = sourceDetails.getId();

            assertNotNull( sourceId );

            // Add a project source
            String insert = MessageFormat.format( projectSourceInsert, sourceId );

            DataScripter script = new DataScripter( insert );
            int rows = script.execute();

            assertEquals( 1, rows );
        }

    }

}

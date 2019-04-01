package wres.io.data.caching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceDetails;
import wres.io.utilities.DataBuilder;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DatabaseConnectionSupplier;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

@RunWith( PowerMockRunner.class )
@PrepareForTest( { SystemSettings.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } ) // thanks https://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class#21268013
public class DataSourcesTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DataSourcesTest.class );

    private static TestDatabase testDatabase;
    private static ComboPooledDataSource dataSource;

    private Connection rawConnection;
    private @Mock DatabaseConnectionSupplier mockConnectionSupplier;
    private Database liquibaseDatabase;

    @BeforeClass
    public static void setup()
    {
        LOGGER.debug( "'@BeforeClass' started" );
        DataSourcesTest.testDatabase = new TestDatabase( "DataSourcesTest" );
        DataSourcesTest.dataSource = DataSourcesTest.testDatabase.getNewComboPooledDataSource();
        LOGGER.debug( "'@BeforeClass' ended with {}", DataSourcesTest.dataSource );
    }

    @Before
    public void beforeEachTest() throws Exception
    {
        LOGGER.debug( "'@Before' started" );
        this.rawConnection = DriverManager.getConnection( DataSourcesTest.testDatabase.getJdbcString() );

        // Set up a bare bones database with only the schema
        DataSourcesTest.testDatabase.createWresSchema( this.rawConnection );

        // Set up a mock for wherever raw connections are used.
        Mockito.when( this.mockConnectionSupplier.get() ).thenReturn( this.rawConnection );

        // Substitute raw connection where needed:
        PowerMockito.mockStatic( SystemSettings.class );
        PowerMockito.when( SystemSettings.class, "getRawDatabaseConnection" )
                    .thenReturn( this.rawConnection );

        PowerMockito.whenNew( DatabaseConnectionSupplier.class )
                    .withNoArguments()
                    .thenReturn( this.mockConnectionSupplier );

        // Substitute our H2 connection pool for both pools:
        PowerMockito.when( SystemSettings.class, "getConnectionPool" )
                    .thenReturn( DataSourcesTest.dataSource );
        PowerMockito.when( SystemSettings.class, "getHighPriorityConnectionPool" )
                    .thenReturn( DataSourcesTest.dataSource );

        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = DataSourcesTest.testDatabase.createNewLiquibaseDatabase( this.rawConnection );
        LOGGER.debug( "'@Before' ended" );
    }

    private static void initializeDataSources() throws Exception
    {
        LOGGER.debug( "initializeDataSources started" );
        DataProvider data = DataBuilder.with( "output_time", "path", "hash", "is_point_data", "source_id" )
                                       .addRow( "2018-08-08T00:00:00Z",
                                                "/somewhere/somewhere/1.ext",
                                                "1234",
                                                false,
                                                1 )
                                       .addRow( "2018-08-08T01:00:00Z",
                                                "/somewhere/somewhere/2.ext",
                                                "12345",
                                                false,
                                                2 )
                                       .addRow( "2018-08-08T02:00:00Z",
                                                "/somewhere/somewhere/3.ext",
                                                "123456",
                                                false,
                                                3 )
                                       .build();
        DataSources dataSources = new DataSources();
        Whitebox.invokeMethod( dataSources, "populate", data );

        Whitebox.setInternalState( DataSources.class, "INSTANCE", dataSources );
        LOGGER.debug( "initializeDataSources ended" );
    }

    @Test
    public void getTwiceFromDataSources()
            throws SQLException, URISyntaxException, LiquibaseException
    {
        LOGGER.debug( "getTwiceFromDataSources began" );

        // Add the source table
        Liquibase liquibase = new Liquibase( "database/wres.Source_v5.xml",
                                             new ClassLoaderResourceAccessor(),
                                             this.liquibaseDatabase );
        liquibase.update( new Contexts() );

        final URI path = new URI( "/this/is/just/a/test" );
        final String time = "2017-06-16 11:13:00";

        Integer result = DataSources.getSourceID(path, time, null, "deadbeef");

        assertTrue("The id should be an integer greater than zero.",
                   result > 0);

        Integer result2 = DataSources.getSourceID(path, time, null, "deadbeef");

        assertEquals("Getting an id with the same path and time should yield the same result.",
                     result2, result);

        int countOfRows;

        try ( Statement statement = this.rawConnection.createStatement();
              ResultSet r = statement.executeQuery( "SELECT COUNT( source_id ) FROM wres.Source" ) )
        {
            r.next();
            countOfRows = r.getInt(1);
        }

        assertEquals("There should be only one row in the wres.Source table",
                     1, countOfRows);

        // Remove the source table etc. now that assertions have finished.
        try ( Statement statement = this.rawConnection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Source" );
        }

        DataSourcesTest.testDatabase.dropLiquibaseChangeTables( this.rawConnection );

        LOGGER.debug( "getTwiceFromDataSources ended" );
    }

    @Test
    public void initializeCacheWithExistingData()
            throws URISyntaxException, SQLException, LiquibaseException
    {
        LOGGER.debug( "initializeCacheWithExistingData began" );

        // Add the source table
        Liquibase liquibase = new Liquibase( "database/wres.Source_v5.xml",
                                             new ClassLoaderResourceAccessor(),
                                             this.liquibaseDatabase );
        liquibase.update( new Contexts() );

        // Create one cache that inserts data to set us up for 2nd cache init.
        DataSources sc = new DataSources();

        final URI path = new URI( "/this/is/just/a/test" );
        final String time = "2017-06-20 16:55:00";
        Integer firstId = sc.getID(path, time, null, "deadbeef");

        // Initialize a second cache, it should find the same data already present
        DataSources scTwo = new DataSources();

        Integer secondId = scTwo.getID(path, time, null, "deadbeef");

        assertEquals("Second cache should find id in database from first cache",
                    firstId, secondId);

        // Remove the source table now that assertions have finished.
        try ( Statement statement = this.rawConnection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Source; " );
        }

        DataSourcesTest.testDatabase.dropLiquibaseChangeTables( this.rawConnection );

        LOGGER.debug( "initializeCacheWithExistingData ended" );
    }

    @Test
    public void testAccess() throws Exception
    {
        DataSourcesTest.initializeDataSources();
        SourceDetails firstDetails = DataSources.getById( 1 );

        Assert.assertNotEquals( firstDetails, null );

        Assert.assertEquals(firstDetails.getId(), (Integer)1);
        Assert.assertFalse(firstDetails.getIsPointData());
        Assert.assertFalse(firstDetails.performedInsert());
        Assert.assertEquals( firstDetails.getHash(), "1234" );

        SourceDetails.SourceKey firstKey = firstDetails.getKey();
        Assert.assertEquals( new URI( "/somewhere/somewhere/1.ext" ), firstKey.getSourcePath() );
        Assert.assertNull( firstKey.getLead() );
        Assert.assertEquals( firstKey.getSourceTime(), "2018-08-08T00:00:00Z");
        Assert.assertEquals( firstKey.getHash(), firstDetails.getHash() );

        SourceDetails secondDetails = null;

        try
        {
            secondDetails = DataSources.get(
                    new URI( "/somewhere/somewhere/1.ext" ),
                    "2018-08-08T00:00:00Z",
                    null,
                    "1234" );
        }
        catch(SQLException e)
        {
            Assert.fail("The DataSources cache tried to retrieve data "
                        + "from the database and failed, when it shouldn't "
                        + "have tried to go in the first place.");
        }

        Assert.assertNotEquals( secondDetails, null );

        Assert.assertEquals( firstDetails.getId(), secondDetails.getId() );
        Assert.assertEquals( firstDetails.getHash(), secondDetails.getHash() );
        Assert.assertEquals( firstDetails.getIsPointData(), secondDetails.getIsPointData() );

        SourceDetails.SourceKey secondKey = secondDetails.getKey();

        Assert.assertEquals( firstKey.getHash(), secondKey.getHash() );
        Assert.assertEquals( firstKey.getSourceTime(), secondKey.getSourceTime() );
        Assert.assertEquals( firstKey.getSourcePath(), secondKey.getSourcePath() );
        Assert.assertEquals(firstKey.getLead(), secondKey.getLead());

        Assert.assertEquals( firstKey, secondKey );

        Assert.assertEquals(firstDetails, secondDetails);

        SourceDetails thirdDetails = DataSources.getById( 3 );

        Assert.assertNotEquals( thirdDetails, null );

        Assert.assertEquals(thirdDetails.getId(), (Integer)3);
        Assert.assertFalse(thirdDetails.getIsPointData());
        Assert.assertFalse(thirdDetails.performedInsert());
        Assert.assertEquals( thirdDetails.getHash(), "123456" );

        SourceDetails.SourceKey thirdKey = thirdDetails.getKey();
        Assert.assertEquals( new URI ("/somewhere/somewhere/3.ext" ), thirdKey.getSourcePath() );
        Assert.assertNull( thirdKey.getLead() );
        Assert.assertEquals( thirdKey.getSourceTime(), "2018-08-08T02:00:00Z");
        Assert.assertEquals( thirdKey.getHash(), thirdDetails.getHash() );

        Assert.assertNotEquals( secondKey.getSourcePath(), thirdKey.getSourcePath());
        Assert.assertNotEquals( secondKey.getSourceTime(), thirdKey.getSourceTime());
        Assert.assertNotEquals( secondKey.getHash(), thirdKey.getHash());

        Assert.assertNotEquals( secondKey, thirdKey );

        Assert.assertNotEquals(secondDetails.getHash(), thirdDetails.getHash());
        Assert.assertNotEquals(secondDetails.getId(), thirdDetails.getId());

        Assert.assertNotEquals( secondDetails, thirdDetails );

        Assert.assertEquals(-1, secondDetails.compareTo(thirdDetails));
        Assert.assertEquals(1, thirdDetails.compareTo( secondDetails ));
    }

    @After
    public void afterEachTest() throws SQLException
    {
        LOGGER.debug( "'@After' began" );
        DataSourcesTest.testDatabase.dropWresSchema( this.rawConnection );
        this.rawConnection.close();
        this.rawConnection = null;
        LOGGER.debug( "'@After' ended" );
    }

    @AfterClass
    public static void tearDown()
    {
        LOGGER.debug( "'@AfterClass' began" );
        DataSourcesTest.dataSource.close();
        DataSourcesTest.dataSource = null;
        DataSourcesTest.testDatabase = null;
        LOGGER.debug( "'@AfterClass' ended" );
    }
}

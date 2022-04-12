package wres.io.data.caching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import com.zaxxer.hikari.HikariDataSource;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceDetails;
import wres.io.utilities.DataBuilder;
import wres.io.utilities.DataProvider;
import wres.io.utilities.TestDatabase;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

public class DataSourcesTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DataSourcesTest.class );
    private static final Random RANDOM = new Random( 523 );

    private TestDatabase testDatabase;
    private HikariDataSource dataSource;

    private @Mock SystemSettings mockSystemSettings;
    private wres.io.utilities.Database wresDatabase;

    private Connection rawConnection;
    private Database liquibaseDatabase;

    @Before
    public void beforeEachTest() throws Exception
    {
        LOGGER.debug( "'@Before' started" );
        MockitoAnnotations.openMocks(  this );
        this.testDatabase = new TestDatabase( "DataSourcesTest"
                                              + RANDOM.nextLong() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );

        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = this.testDatabase.createNewLiquibaseDatabase( this.rawConnection );
        // Set up a bare bones database with only the schema
        this.testDatabase.createWresSchema( this.rawConnection );

        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockSystemSettings.getMaximumPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        LOGGER.debug( "'@Before' ended" );
    }

    private DataSources initializeDataSources()
    {
        LOGGER.debug( "initializeDataSources started" );
        DataProvider data = DataBuilder.with( "path", "hash", "is_point_data", "source_id", "variable_name", "feature_id", "measurementunit_id", "timescale_id" )
                                       .addRow( "/somewhere/somewhere/1.ext",
                                                "1234",
                                                false,
                                                1,
                                                "variable1",
                                                1,
                                                1,
                                                1 )
                                       .addRow( "/somewhere/somewhere/2.ext",
                                                "12345",
                                                false,
                                                2,
                                                "variable2",
                                                2,
                                                2,
                                                2 )
                                       .addRow( "/somewhere/somewhere/3.ext",
                                                "123456",
                                                false,
                                                3,
                                                "variable3",
                                                3,
                                                3,
                                                3 )
                                       .build();
        DataSources dataSources = new DataSources( this.wresDatabase );
        dataSources.populate( data );
        LOGGER.debug( "initializeDataSources ended" );
        return dataSources;
    }

    @Test
    public void getTwiceFromDataSources()
            throws SQLException, LiquibaseException
    {
        LOGGER.debug( "getTwiceFromDataSources began" );

        // Add the source table
        this.testDatabase.createSourceTable( this.liquibaseDatabase );

        DataSources dataSourcesCache = new DataSources( this.wresDatabase );
        SourceDetails sourceDetailsOne = new SourceDetails( "deadbeef" );
        sourceDetailsOne.setVariableName( "V" );
        sourceDetailsOne.setMeasurementUnitId( 1L );
        sourceDetailsOne.setFeatureId( 1L );
        sourceDetailsOne.save( this.wresDatabase );
        Long result = dataSourcesCache.getSourceID( "deadbeef" );

        assertTrue("The id should be an integer greater than zero.",
                   result > 0);

        Long result2 = dataSourcesCache.getSourceID( "deadbeef");

        assertEquals("Getting an id with the same hash should yield the same result.",
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
        this.testDatabase.dropSourceTable( this.rawConnection );
        this.testDatabase.dropLiquibaseChangeTables( this.rawConnection );

        LOGGER.debug( "getTwiceFromDataSources ended" );
    }

    @Test
    public void initializeCacheWithExistingData()
            throws URISyntaxException, SQLException, LiquibaseException
    {
        LOGGER.debug( "initializeCacheWithExistingData began" );

        // Add the source table
        this.testDatabase.createSourceTable( this.liquibaseDatabase );

        // Create one cache that inserts data to set us up for 2nd cache init.
        DataSources sc = new DataSources( this.wresDatabase );
        SourceDetails sourceDetailsOne = new SourceDetails( "deadbeef" );
        sourceDetailsOne.setVariableName( "V" );
        sourceDetailsOne.setMeasurementUnitId( 1L );
        sourceDetailsOne.setFeatureId( 1L );
        sourceDetailsOne.save( this.wresDatabase );

        Long firstId = sc.getID( "deadbeef" );

        // Initialize a second cache, it should find the same data already present
        DataSources scTwo = new DataSources( this.wresDatabase );

        Long secondId = scTwo.getID( "deadbeef" );

        assertEquals("Second cache should find id in database from first cache",
                    firstId, secondId);

        // Remove the source table now that assertions have finished.
        this.testDatabase.dropSourceTable( this.rawConnection );
        this.testDatabase.dropLiquibaseChangeTables( this.rawConnection );

        LOGGER.debug( "initializeCacheWithExistingData ended" );
    }

    @Test
    public void testAccess() throws Exception
    {
        DataSources dataSourcesCache = this.initializeDataSources();

        // Add the source table
        this.testDatabase.createSourceTable( this.liquibaseDatabase );
        SourceDetails firstDetails = dataSourcesCache.getById( 1L );

        Assert.assertNotEquals( firstDetails, null );

        Assert.assertEquals( firstDetails.getId(), (Long) 1L );
        Assert.assertFalse(firstDetails.getIsPointData());
        Assert.assertFalse(firstDetails.performedInsert());
        Assert.assertEquals( firstDetails.getHash(), "1234" );

        SourceDetails secondDetails = null;

        try
        {
            secondDetails = dataSourcesCache.get( "1234" );
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

        String secondKey = secondDetails.getKey();

        Assert.assertEquals(firstDetails, secondDetails);

        SourceDetails thirdDetails = dataSourcesCache.getById( 3L );

        Assert.assertNotEquals( thirdDetails, null );

        Assert.assertEquals(thirdDetails.getId(), (Long) 3L);
        Assert.assertFalse(thirdDetails.getIsPointData());
        Assert.assertFalse(thirdDetails.performedInsert());
        Assert.assertEquals( thirdDetails.getHash(), "123456" );

        String thirdKey = thirdDetails.getKey();


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
        this.wresDatabase.shutdown();
        this.testDatabase.dropWresSchema( this.rawConnection );
        this.rawConnection.close();
        this.rawConnection = null;
        this.dataSource.close();
        this.dataSource = null;
        this.testDatabase = null;
        LOGGER.debug( "'@After' ended" );
    }
}

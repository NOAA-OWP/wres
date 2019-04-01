package wres.io.utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( QueryTest.class);

    // We need a way to control faked out database connections
    private static TestDatabase testDatabase;
    private static ComboPooledDataSource dataSource;

    // We need a raw connection to use to run setup/teardown operations on.
    private Connection rawConnection;

    // We need a liquibase instance to use to build up tables.
    private liquibase.database.Database liquibaseDatabase;

    @BeforeClass
    public static void setup()
    {
        LOGGER.trace( "@BeforeClass began" );
        // We need to create a test database so we aren't trying to reach out to a real, deployed database
        QueryTest.testDatabase = new TestDatabase( "QueryTest" );
        QueryTest.dataSource = QueryTest.testDatabase.getNewComboPooledDataSource();
        LOGGER.trace( "@BeforeClass ended" );
    }

    @Before
    public void beforeEachTest() throws SQLException, DatabaseException
    {
        LOGGER.trace( "@Before began" );
        this.rawConnection = DriverManager.getConnection( QueryTest.testDatabase.getJdbcString() );

        // Set up a bare bones database with only the schema
        QueryTest.testDatabase.createWresSchema( this.rawConnection );

        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = QueryTest.testDatabase.createNewLiquibaseDatabase( this.rawConnection );
        LOGGER.trace( "@Before ended" );
    }


    /**
     * Test to see if a Query can execute a single static query in the database and return the desired results
     * @throws SQLException Thrown if a connection could not be created
     * @throws SQLException Thrown if the query 'Select 1 as one;' cannot be called
     * @throws SQLException Thrown if an int in the "one" column could not be retrieved
     */

    @Test
    public void simpleCallTest() throws SQLException
    {
        // All we need to do is retrieve some value that we can test against
        String script = "SELECT 1 AS one;";

        try ( Connection connection = QueryTest.dataSource.getConnection();
              ResultSet results = Query.withScript( script ).call( connection ) )
        {
            // False means that nothing was retrieved
            Assert.assertTrue( results.isBeforeFirst() );

            // JDBC requires that we move to the first record in the result set
            results.next();

            // We told it to give us the value 1 as "one", so that should be what we get back
            int value = results.getInt( "one" );

            Assert.assertEquals( 1, value );
        }
    }


    /**
     * Test to see if a Query can execute a single parameterized query in the database and return the
     * desired results. This is identical to simpleCallTest except that we pass "1" as a parameter rather
     * than having it baked into the script.
     * @throws SQLException Thrown if a connection could not be created
     * @throws SQLException Thrown if the query 'Select ? as one;' cannot be called
     * @throws SQLException Thrown if an int in the "one" column could not be retrieved
     */

    @Test
    public void parameterizedCallTest() throws SQLException
    {
        // "?" is a placeholder for a parameter
        String script = "Select ? as one;";

        Query testQuery = Query.withScript( script ).setParameters( 1 );

        try ( Connection connection = QueryTest.dataSource.getConnection();
              ResultSet results = testQuery.call( connection ) )
        {
            // False means that nothing was retrieved
            Assert.assertTrue( "No values were returned from the query.", results.isBeforeFirst() );

            // JDBC requires that we move to the first record in the result set
            results.next();

            // We told it to give us the value 1 as "one", so that should be what we get back
            int value = results.getInt( "one" );

            Assert.assertEquals( value, 1 );
        }
    }


    /**
     * Tests to see if a simple query can be executed
     * @throws SQLException Thrown if a connection could not be created
     * @throws SQLException Thrown if a statement could not be run in the database
     * @throws SQLException Thrown if values could not be retrieved from the results
     */

    @Test
    public void simpleExecuteTest() throws SQLException, LiquibaseException
    {
        // Since wres.Project is such a simple table, we're going to use that one to test

        // Add the project table
        QueryTest.testDatabase.createProjectTable( this.liquibaseDatabase );

        String script = "INSERT INTO wres.Project(input_code, project_name) VALUES (0, 'zero');";

        Query testQuery = Query.withScript( script );

        try ( Connection connection = QueryTest.dataSource.getConnection() )
        {
            // Since execute does all of the interior closing, we don't need to do anything special to
            // make sure that there isn't an open statement anywhere (nor can we)
            int insertedRows = testQuery.execute( connection );

            // One row should have been inserted
            Assert.assertEquals( 1, insertedRows );

            // To check to see if we've added anything
            testQuery = Query.withScript( "SELECT input_code, project_name FROM wres.Project;" );

            try (ResultSet projects = testQuery.call( connection ))
            {
                // If "isBeforeFirst" is true, it means that there is at least one entry returned,
                // meaning that inserts occurred as requested
                Assert.assertTrue( "No data was added to check.", projects.isBeforeFirst() );

                // We need to call "next" to step into the data
                projects.next();

                // The values we sent into the insert should show up in the results
                Assert.assertEquals( 0, projects.getInt( "input_code" ) );
                Assert.assertEquals( "zero", projects.getString("project_name") );

                // If there is anything left, it means that too much data got added
                Assert.assertFalse(projects.next());
            }
        }

        // Remove the project table and liquibase tables
        QueryTest.testDatabase.dropProjectTable( this.rawConnection );
        QueryTest.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }


    /**
     * Test to see if many values can be inserted into the database with a single batch execution
     * @throws SQLException Thrown if a connection could not be created
     * @throws SQLException Thrown if the query 'INSERT INTO wres.Project(input_code, project_name) VALUES (?, ?);'
     * cannot be called
     * @throws SQLException Thrown if the given parameter values cannot be used in the query
     * @throws SQLException Thrown if no data was returned for examination
     * @throws SQLException Thrown if the values to check could not be properly retrieved from the result set
     */

    @Test
    public void batchExecuteTest() throws SQLException, LiquibaseException
    {
        // Since wres.Project is such a simple table, we're going to use that one to test

        // Add the project table
        QueryTest.testDatabase.createProjectTable( this.liquibaseDatabase );

        String script = "INSERT INTO wres.Project(input_code, project_name) VALUES (?, ?);";

        // Since we're going to run this as batch, we need a ton of parameters to pass in
        List<Object[]> arguments = new ArrayList<>(  );

        arguments.add(new Object[]{0, "zero"});
        arguments.add(new Object[]{1, "one"});
        arguments.add(new Object[]{2, "two"});
        arguments.add(new Object[]{3, "three"});
        arguments.add(new Object[]{4, "four"});
        arguments.add(new Object[]{5, null});

        Query testQuery = Query.withScript( script ).setBatchParameters( arguments );

        try ( Connection connection = QueryTest.dataSource.getConnection() )
        {
            // Since execute does all of the interior closing, we don't need to do anything special to
            // make sure that there isn't an open statement anywhere (nor can we)
            int insertedRows = testQuery.execute( connection );

            // There were six sets of parameters, so there should have been six inserted rows
            Assert.assertEquals( 6, insertedRows );

            // To check to see if we've added anything,
            testQuery = Query.withScript( "SELECT input_code, project_name FROM wres.Project;" );

            try (ResultSet projects = testQuery.call( connection ))
            {
                // If "isBeforeFirst" is true, it means that there is at least one entry returned,
                // meaning that inserts occurred as requested
                Assert.assertTrue( "No values were previously added.", projects.isBeforeFirst() );

                // Inserts alone doesn't mean it works, so we need to go through each row and check the arguments
                // This was done in batch, the values should still be in order
                for (Object[] argument : arguments)
                {
                    projects.next();
                    Assert.assertEquals( argument[0], projects.getInt( "input_code" ) );
                    Assert.assertEquals( argument[1], projects.getString( "project_name" ) );
                }

                // If there is anything left, it means that too much data got added
                Assert.assertFalse("Too many values were returned.", projects.next());
            }
        }

        // Remove the project table and liquibase tables
        QueryTest.testDatabase.dropProjectTable( this.rawConnection );
        QueryTest.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }


    /**
     * Test to see if values may be added to the database with a parameterized statement
     * @throws SQLException Thrown if a connection could not be created
     * @throws SQLException Thrown if either the query used to add a value or the query used to check the
     * initial query failed to execute properly
     * @throws SQLException Thrown if no data was returned for examination
     * @throws SQLException Thrown if the values to check could not be properly retrieved from the result set
     */

    @Test
    public void parameterizedExecuteTest() throws SQLException, LiquibaseException
    {
        // Since wres.Project is such a simple table, we're going to use that one to test

        // Add the project table
        QueryTest.testDatabase.createProjectTable( this.liquibaseDatabase );

        String script = "INSERT INTO wres.Project(input_code, project_name) VALUES (?, ?);";

        Query testQuery = Query.withScript( script ).setParameters(0, "zero");

        try ( Connection connection = QueryTest.dataSource.getConnection() )
        {
            // Since execute does all of the interior closing, we don't need to do anything special to
            // make sure that there isn't an open statement anywhere (nor can we)
            int insertedRows = testQuery.execute( connection );

            // Since the script is set to insert one value, it should have inserted one value in the database
            Assert.assertEquals( 1, insertedRows );

            // To check to see if we've added anything, we can go ahead and do that by removing everything.
            // This will remove everything and select it all at the same time
            testQuery = Query.withScript( "SELECT input_code, project_name FROM wres.Project;" );

            try (ResultSet projects = testQuery.call( connection ))
            {
                // If "isBeforeFirst" is true, it means that there is at least one entry returned,
                // meaning that inserts occurred as requested
                Assert.assertTrue( projects.isBeforeFirst() );

                int entryCount = 0;

                while (projects.next())
                {
                    entryCount++;
                    Assert.assertEquals( 0, projects.getInt("input_code") );
                    Assert.assertEquals( "zero", projects.getString( "project_name" ) );
                }

                Assert.assertEquals( 1, entryCount );
            }
        }

        // Remove the project table and liquibase tables
        QueryTest.testDatabase.dropProjectTable( this.rawConnection );
        QueryTest.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }


    /**
     * The status of a connection's autocommit setting should be the same before and after the query
     * execution/call, regardless of whether or not the query itself was supposed to run in a transaction
     * @throws SQLException Thrown when a connection could not be retrieved
     * @throws SQLException Thrown if the query could not use the connection to execute the script
     * @throws SQLException Thrown if the state of the connection's autocommit could not be evaluated
     */

    @Test
    public void maintainAutoCommitTest() throws SQLException
    {
        Query testQuery = Query.withScript( "SELECT 1;" ).inTransaction( true );

        try ( Connection connection = dataSource.getConnection() )
        {
            // Call the query. It is supposed to run in a transaction, but the connection wasn't set to be in an open
            // transaction. The connection should come back with autocommit active
            testQuery.call( connection );

            Assert.assertTrue( "Autocommit should still be turned on, but the query "
                               + "kept it off after its transaction.",
                               connection.getAutoCommit() );

            // If we set the connection to run in a transaction, it should come back without autocommit turned on
            connection.setAutoCommit( false );
            testQuery.inTransaction( false );

            testQuery.call( connection );

            Assert.assertFalse( "Autocommit was supposed to be turned off, but the query "
                                + "somehow turned it back on.",
                                connection.getAutoCommit() );
        }
    }

    @After
    public void afterEachTest() throws SQLException
    {
        LOGGER.trace( "@After began" );
        QueryTest.testDatabase.dropWresSchema( this.rawConnection );
        this.rawConnection.close();
        this.rawConnection = null;
        LOGGER.trace( "@After ended" );
    }

    @AfterClass
    public static void tearDown()
    {
        LOGGER.trace( "@AfterClass began" );
        QueryTest.dataSource.close();
        QueryTest.dataSource = null;
        QueryTest.testDatabase = null;
        LOGGER.trace( "@AfterClass ended" );
    }
}

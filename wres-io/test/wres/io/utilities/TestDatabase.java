package wres.io.utilities;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * Exposes helpers that JUnit tests can use to manipulate testRuntime databases.
 * <br>
 * A test or test class can create and destroy one helper per test database.
 * <br>
 * Callers are expected to close any resources created by this class, such as
 * data sources, schemas, and so forth. No state of the database is stored here,
 * state is expected to be managed by the test classes themselves, and this is
 * here to reduce the repetition of exact implementation of setup and tear down
 * methods that are frequently used by tests.
 */

public class TestDatabase
{
    private final String name;

    /**
     * A name to separate this test database from another, use a test method or
     * test class name here.
     * @param name the name to distinguish test databases from one another.
     * @throws NullPointerException when name is null
     * @throws IllegalArgumentException when name is blank
     */

    public TestDatabase( String name )
    {
        Objects.requireNonNull( name );

        if ( name.isBlank() )
        {
            throw new IllegalArgumentException( "Name must not be blank." );
        }

        this.name = name;
    }

    /**
     * Create a new ComboPooledDataSource that uses this instance's name.
     *
     * The idea is to isolate tests from one another by using separate database
     * instances, one way to do that is to use a different database name.
     * @return a newly created ComboPooledDataSource
     */

    public ComboPooledDataSource getNewComboPooledDataSource()
    {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl( this.getJdbcString() );
        dataSource.setAutoCommitOnClose( true );
        dataSource.setInitialPoolSize( 10 );
        dataSource.setMaxIdleTime( 30 );
        dataSource.setMaxPoolSize( 20 );
        dataSource.setUnreturnedConnectionTimeout( 1 );
        dataSource.setDebugUnreturnedConnectionStackTraces( true );
        return dataSource;
    }

    /**
     * Get a jdbc connection string for this test database.
     * @return the jdbc connection string based on this database's name.
     * @throws IllegalStateException when the jdbc driver cannot be found.
     */

    public String getJdbcString()
    {
        try
        {
            Class.forName( "org.h2.Driver" );
        }
        catch ( ClassNotFoundException cnfe )
        {
            throw new IllegalStateException( "Couldn't find h2 driver.", cnfe );
        }

        //return "jdbc:h2:mem:" + this.name + ";DB_CLOSE_DELAY=-1";

        // helps h2 use a subset of postgres' syntax or features:
        //return "jdbc:h2:mem:" + this.name + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

        // Use this verbose one to figure out issues with queries/files/h2/etc:
        //return "jdbc:h2:mem:" + this.name + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;TRACE_LEVEL_SYSTEM_OUT=3";
        //return "jdbc:h2:mem:" + this.name + ";MODE=PostgreSQL;TRACE_LEVEL_SYSTEM_OUT=3";

        return "jdbc:h2:mem:" + this.name + ";MODE=PostgreSQL;";
    }

    /**
     * Create a schema on a given test database connection.
     * Caller must subsequently call dropWresSchema at the same level.
     * For example, if you call createWresSchema in @Before, you must call
     * dropWresSchema in @After.
     * @param connection the connection to use to create the schema
     * @throws SQLException when creation fails
     */

    public void createWresSchema( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "CREATE SCHEMA wres" );
        }
    }

    /**
     * Drop a schema on a given test database connection.
     * Caller must have previously called createWresSchema at the same level.
     * For example, if you call createWresSchema in @Before, you must call
     * dropWresSchema in @After.
     * @param connection the connection to use to drop the schema
     * @throws SQLException when dropping fails
     */

    public void dropWresSchema( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP SCHEMA wres CASCADE" );
        }
    }


    /**
     * Create the WRES projects table using liquibase.
     * Expected to be called within a test requiring the projects table. If you
     * call createProjectsTable at the beginning of a test, you must call
     * dropProjectsTable at the end of the test. You must also call
     * dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createProjectsTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.Project_v2.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );
    }

    /**
     * Drop the WRES projects table on a connection.
     * Expected to be called at the same level as createProjectsTable, namely
     * within a test that requires the projects table, at the end of the test.
     * @param connection the connection to use
     * @throws SQLException when dropping fails
     */
    public void dropProjectsTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Project" );
        }
    }

    /**
     * Create a liquibase database instance to be used to create tables.
     *
     * @param connection the connection to use for liquibase migrations
     * @return the liquibase database instance
     * @throws DatabaseException when findCorrectDatabaseImplementation fails
     */

    public Database createNewLiquibaseDatabase( Connection connection )
            throws DatabaseException
    {
        JdbcConnection liquibaseConnection = new JdbcConnection( connection );
        return DatabaseFactory.getInstance()
                              .findCorrectDatabaseImplementation( liquibaseConnection );
    }


    /**
     * Drop liquibase change tables, in other words, cause liquibase to forget
     * about its prior migrations entirely.
     *
     * You must call this at the same level if you created any database tables
     * using liquibase. For example, you create the projects table using
     * createProjectsTable, you ran your test and asserted everything, then you
     * called dropProjectsTable, but liquibase still has a memory of the
     * migration that created the projects table. So you call this to have it
     * forget.
     * @param connection the connection to use
     * @throws SQLException when dropping fails
     */

    public void dropLiquibaseChangeTables( Connection connection )
            throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE public.databasechangelog; DROP TABLE public.databasechangeloglock;" );
        }
    }
}

package wres.io.utilities;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
 * 
 * TODO: there are a LOT of resource leaks in this class. Return the resources 
 * for the caller to close (JBr).
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
     * Create a new data source that uses this instance's name.
     *
     * The idea is to isolate tests from one another by using separate database
     * instances, one way to do that is to use a different database name.
     * @return a newly created ComboPooledDataSource
     */

    public HikariDataSource getNewHikariDataSource()
    {
        HikariConfig poolConfig = new HikariConfig();
        String url = this.getJdbcString();
        poolConfig.setJdbcUrl( url );
        poolConfig.setMaximumPoolSize( 10 );
        poolConfig.setConnectionTimeout( 0 );
        return new HikariDataSource( poolConfig );
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
            Class.forName( "org.h2.jdbcx.JdbcDataSource" );
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

        return "jdbc:h2:mem:" + this.name + ";MODE=PostgreSQL;TRACE_LEVEL_FILE=4";
    }

    /**
     * Create a schema on a given test database connection.
     * Caller must subsequently call dropWresSchema at the same level.
     * For example, if you call createWresSchema in @Before, you must call
     * dropWresSchema in @After.
     * @param connection the connection to use to create the schema
     * @throws SQLException when create fails
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
     * @throws SQLException when drop fails
     */

    public void dropWresSchema( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP SCHEMA wres CASCADE" );
        }
    }


    /**
     * Create the WRES project table using given liquibase database.
     * Expected to be called within a test requiring the project table. If you
     * call createProjectTable at the beginning of a test, you must call
     * dropProjectTable at the end of the test. You must also call
     * dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createProjectTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.Project_v5.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );
    }


    /**
     * Drop the WRES project table using given connection.
     * Expected to be called at the same level as createProjectTable, namely
     * within a test that requires the project table, at the end of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */

    public void dropProjectTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Project" );
        }
    }


    /**
     * Create the WRES source table using given liquibase database.
     * Expected to be called within a test requiring the source table. If you
     * call createSourceTable at the beginning of a test, you must call
     * dropSourceTable at the end of the test. You must also call
     * dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createSourceTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.Source_v7.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );

    }


    /**
     * Drop the WRES source table using given connection.
     * Expected to be called at the same level as createSourceTable, namely
     * within a test that requires the sources table, at the end of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */
    public void dropSourceTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Source" );
        }
    }
    
    
    /**
     * Create the WRES source table using given liquibase database.
     * Expected to be called within a test requiring the projectsource table. 
     * If you call createProjectSourceTable at the beginning of a test, you 
     * must call dropProjectSourceTable at the end of the test. You must also 
     * call dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createProjectSourceTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.ProjectSource_v6.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );

    }


    /**
     * Drop the WRES source table using given connection.
     * Expected to be called at the same level as createProjectSourceTable, namely
     * within a test that requires the projectsource table, at the end of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */
    public void dropProjectSourceTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.ProjectSource" );
        }
    }        


    /**
     * Create the WRES source table using given liquibase database.
     * Expected to be called within a test requiring the feature table. If you
     * call createFeatureTable at the beginning of a test, you must call
     * dropFeatureTable at the end of the test. You must also call
     * dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createFeatureTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.Feature_v3.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );

    }


    /**
     * Drop the WRES source table using given connection.
     * Expected to be called at the same level as createFeatureTable, namely
     * within a test that requires the feature table, at the end of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */
    public void dropFeatureTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Feature" );
        }
    }  
    

    
    /**
     * Create the WRES measurementunit table using given liquibase database.
     * Expected to be called within a test requiring the measurementunit table. 
     * If you call createMeasurementUnitTable at the beginning of a test, you 
     * must call dropMeasurementUnitTable at the end of the test. You must also call
     * dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createMeasurementUnitTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.measurementunit_v1.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );
    }


    /**
     * Drop the WRES measurementunit table using given connection.
     * Expected to be called at the same level as createMeasurementUnitTable, 
     * namely within a test that requires the measurementunit table, at the end 
     * of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */

    public void dropMeasurementUnitTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.MeasurementUnit" );
        }
    }


    /**
     * Create the WRES ensemble table using given liquibase database.
     * Expected to be called within a test requiring the ensemble table. 
     * If you call createEnsembleTable at the beginning of a test, you 
     * must call dropEnsembleTable at the end of the test. You must also call
     * dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createEnsembleTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.Ensemble_v4.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );
    }


    /**
     * Drop the WRES ensemble table using given connection.
     * Expected to be called at the same level as createEnsembleTable, 
     * namely within a test that requires the ensemble table, at the end 
     * of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */

    public void dropEnsembleTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.Ensemble" );
        }
    }

    
    /**
     * Create the WRES timeseries table using given liquibase database.
     * Expected to be called within a test requiring the timeseries table. 
     * If you call createTimeSeriesTable at the beginning of a test, you 
     * must call dropTimeSeriesTable at the end of the test. You must also 
     * call dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createTimeSeriesTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.TimeSeries_v4.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );
    }


    /**
     * Drop the WRES timeseries table using given connection.
     * Expected to be called at the same level as createTimeSeriesTable, 
     * namely within a test that requires the timeseries table, at the end 
     * of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */

    public void dropTimeSeriesTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.TimeSeries" );
        }
    }     
    
    
    /**
     * Create the WRES timeseriesvalue table using given liquibase database.
     * Expected to be called within a test requiring the timeseriesvalue table. 
     * If you call createTimeSeriesValueTable at the beginning of a test, you 
     * must call dropTimeSeriesValue at the end of the test. You must also 
     * call dropLiquibaseChangeTables at the end of the test.
     * @param liquibaseDatabase the Liquibase Database instance to use
     * @throws LiquibaseException when liquibase migration fails
     */

    public void createTimeSeriesValueTable( Database liquibaseDatabase )
            throws LiquibaseException
    {
        Liquibase liquibase = new Liquibase( "database/wres.TimeSeriesValue_v2.xml",
                                             new ClassLoaderResourceAccessor(),
                                             liquibaseDatabase );
        liquibase.update( new Contexts() );
    }


    /**
     * Drop the WRES timeseriesvalue table using given connection.
     * Expected to be called at the same level as createTimeSeriesValueTable, 
     * namely within a test that requires the timeseriesvalue table, at the end 
     * of the test.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */

    public void dropTimeSeriesValueTable( Connection connection ) throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE wres.TimeSeriesValue" );
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
     * createProjectTable, you ran your test and asserted everything, then you
     * called dropProjectTable, but liquibase still has a memory of the
     * migration that created the projects table. So you call this to have it
     * forget.
     * @param connection the connection to use
     * @throws SQLException when drop fails
     */

    public void dropLiquibaseChangeTables( Connection connection )
            throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP TABLE public.databasechangelog; DROP TABLE public.databasechangeloglock;" );
        }
    }

    public void shutdown( Connection connection )
            throws SQLException
    {
        try ( Statement statement = connection.createStatement() )
        {
            statement.execute( "SHUTDOWN" );
        }
    }
}

package wres.io.database;

import java.io.Closeable;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.database.locking.DatabaseLockFailed;
import wres.io.database.locking.DatabaseLockManager;
import wres.system.DatabaseType;

class DatabaseSchema implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseSchema.class );
    private final DatabaseLockManager lockManager;

    DatabaseSchema( final String databaseName,
                    DatabaseLockManager lockManager )
    {
        this.lockManager = lockManager;

        LOGGER.debug( "Migrating database {}.", databaseName );

        try
        {
            // The companion unlockExclusive is in the close() method which
            // must be called in a "finally" block when this succeeds.
            this.lockManager.lockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( SQLException | DatabaseLockFailed e )
        {
            Error error = new ExceptionInInitializerError( "Failed to mark database as undergoing changes." );
            error.initCause( e );
            throw error;
        }
    }

    /**
     * Returns the change log URL. Left package private for unit testing
     * @return the change log URL
     */
    String getChangelogURL()
    {
        String relativeToClasspath = "database/db.changelog-master.xml";

        // Only to test that it is present:
        URL changelogURL = this.getClass()
                               .getClassLoader()
                               .getResource( relativeToClasspath );

        if( Objects.isNull( changelogURL ) )
        {
            throw new DatabaseMigrationException( "The database migration scripts could not be found at classpath: '"
                                                  + relativeToClasspath + "'" );
        }

        // Prevent inclusion of absolute paths in liquibase changelog by
        // returning the path on the classpath only.
        return relativeToClasspath;
    }

    /**
     * Apply the schema
     * @param connection the connection
     * @throws DatabaseMigrationException if the database failed to migrate
     */
    void applySchema( final Connection connection )
    {
        Database database;
        try
        {
            database = DatabaseFactory.getInstance()
                                      .findCorrectDatabaseImplementation(
                                              new JdbcConnection( connection )
                                      );
        }
        catch ( DatabaseException e )
        {
            throw new DatabaseMigrationException( "A database instance could not be accessed.", e );
        }

        try
        {
            Liquibase liquibase = new Liquibase(
                    this.getChangelogURL(),
                    new ClassLoaderResourceAccessor(),
                    database
            );

            Contexts contexts = new Contexts();
            LabelExpression expression = new LabelExpression();
            liquibase.update( contexts, expression );
        }
        catch ( LiquibaseException e )
        {
            throw new DatabaseMigrationException( "The WRES could not be properly initialized.", e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            this.lockManager.unlockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( "Unable to unlock using "
                                             + DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
    }

    /**
     * A database migration exception.
     */
    private static class DatabaseMigrationException extends RuntimeException
    {
        /**
         * Creates an instance.
         * @param message the message
         */
        public DatabaseMigrationException( String message )
        {
            super( message );
        }

        /**
         * Creates an instance.
         * @param message the message
         * @param cause the cause
         */
        public DatabaseMigrationException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }
}

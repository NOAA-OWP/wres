package wres.system;

import java.io.Closeable;
import java.io.IOException;
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

class DatabaseSchema implements Closeable
{
    private final DatabaseLockManager lockManager;

    DatabaseSchema( final String databaseName,
                    DatabaseLockManager lockManager )
    {
        this.lockManager = lockManager;

        try
        {
            // The companion unlockExclusive is in the close() method which
            // must be called in a "finally" block when this succeeds.
            this.lockManager.lockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( SQLException | DatabaseLockFailed e )
        {
            Error error = new ExceptionInInitializerError( "Failed to mark database as undergoing changes." );
            error.initCause( e );
            throw error;
        }
    }


    // Left public for unit testing
    String getChangelogURL()
    {
        String relativeToClasspath = "database/db.changelog-master.xml";

        // Only to test that it is present:
        URL changelogURL = this.getClass()
                               .getClassLoader()
                               .getResource( relativeToClasspath );
        Objects.requireNonNull( changelogURL,
                                "The definition for the WRES data model could not be found at classpath:'"
                                + relativeToClasspath + "'" );

        // Prevent inclusion of absolute paths in liquibase changelog by
        // returning the path on the classpath only.
        return relativeToClasspath;
    }

    void applySchema( final Connection connection )
            throws SQLException, IOException
    {
        Database database = null;
        try
        {
            database = DatabaseFactory.getInstance()
                                      .findCorrectDatabaseImplementation(
                                                       new JdbcConnection( connection )
                                               );
        }
        catch ( DatabaseException e )
        {
            throw new IOException("A database instance could not be accessed.");
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
        catch (LiquibaseException e)
        {
            throw new SQLException( "The WRES could not be properly initialized.", e);
        }
    }

    @Override
    public void close()
    {
        try
        {
            this.lockManager.unlockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( "Unable to unlock using "
                                             + DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
    }
}

package wres.system;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A nerfed/no-op DatabaseLockManager for in-memory applications (e.g., H2 or no database).
 */

public class DatabaseLockManagerNoop implements DatabaseLockManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseLockManagerNoop.class );

    /**
     * Creates an instance.
     */
    public DatabaseLockManagerNoop()
    {
        LOGGER.warn( "Using no-op database lock manager, appropriate for in-memory evaluations only." );
    }

    @Override
    public void shutdown()
    {
        // No-op
    }

    @Override
    public boolean lockSource( Long lockName ) throws SQLException
    {
        return true;
    }

    @Override
    public boolean unlockSource( Long lockName ) throws SQLException
    {
        return true;
    }

    @Override
    public void lockExclusive( Long lockName ) throws SQLException
    {
        // No-op
    }

    @Override
    public void unlockExclusive( Long lockName ) throws SQLException
    {
        // No-op
    }

    @Override
    public void lockShared( Long lockName ) throws SQLException
    {
        // No-op
    }

    @Override
    public void unlockShared( Long lockName ) throws SQLException
    {
        // No-op
    }
}

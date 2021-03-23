package wres.system;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A nerfed/no-op DatabaseLockManager for H2 in-memory databases.
 */

public class DatabaseLockManagerNoop implements DatabaseLockManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseLockManagerNoop.class );

    public DatabaseLockManagerNoop()
    {
        LOGGER.warn( "Using no-op database lock manager, appropriate for in-memory H2 and testing only." );
    }

    @Override
    public void shutdown()
    {
        // No-op
    }

    @Override
    public void lockSource( Long lockName ) throws SQLException
    {
        // No-op
    }

    @Override
    public void unlockSource( Long lockName ) throws SQLException
    {
        // No-op
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

    @Override
    public boolean isSourceLocked( Long lockName ) throws SQLException
    {
        return false;
    }
}

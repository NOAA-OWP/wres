package wres.io.database.locking;

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
    public boolean lockSource( Long lockName )
    {
        return true;
    }

    @Override
    public boolean unlockSource( Long lockName )
    {
        return true;
    }

    @Override
    public void lockExclusive( Long lockName )
    {
        // No-op
    }

    @Override
    public void unlockExclusive( Long lockName )
    {
        // No-op
    }

    @Override
    public void lockShared( Long lockName )
    {
        // No-op
    }

    @Override
    public void unlockShared( Long lockName )
    {
        // No-op
    }
}

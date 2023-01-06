package wres.system;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * Manages application-level locks on database objects.
 * 
 * These are outside of, or in addition to, the usual locks on tables and rows for queries.
 *
 * If there are different strategies required for locking with one rdbms versus another rdbms, those differences are in 
 * different {@link DatabaseLockManager} implementations.
 *
 * Exactly one instance of this class is expected to be used by each WRES process and shared as needed.
 */

public interface DatabaseLockManager
{
    /**
     * Shutdown the lock manager.
     *
     * @throws SecurityException in the absence of permission
     */

    void shutdown();

    /**
     * Attempts to lock the back-end database using lockName which is a source id.
     * @param lockName the lock name to use, must be non-zero and positive, less than {@link Integer#MAX_VALUE}
     * @return true if the source was locked, false otherwise
     * @throws IllegalArgumentException when lockName less than 1 or is MAX_VALUE
     * @throws DatabaseLockFailed when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    boolean lockSource( Long lockName ) throws SQLException;

    /**
     * Unlock the back-end database using lockName that was previously locked.
     * using lockSource.
     * @param lockName the lock name to use, must be non-zero and positive, less than {@link Integer#MAX_VALUE}, and 
     *            have been previously locked with {@link #lockSource(Long)}
     * @return true if the source was unlocked, false otherwise
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE or was not previously locked
     * @throws DatabaseLockFailed when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails on both attempts.
     */

    boolean unlockSource( Long lockName ) throws SQLException;

    /**
     * Lock the back-end database using exclusive lock with the supplied name.
     * @param lockName the lock name to use, must be non-zero and positive, less than {@link Integer#MAX_VALUE}
     * @throws IllegalArgumentException when lockName less than 1 or is {@link Integer#MAX_VALUE}
     * @throws DatabaseLockFailed when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    void lockExclusive( Long lockName ) throws SQLException;

    /**
     * Unlock the back-end database using lockName that was previously locked.
     * @param lockName the lock name to use, must be non-zero and positive, less than {@link Integer#MAX_VALUE}, and 
     *            have been previously locked with lock()
     * @throws IllegalArgumentException when lockName less than 1 or is {@link Integer#MAX_VALUE} or was not previously 
     *            locked
     * @throws IllegalStateException when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails
     */

    void unlockExclusive( Long lockName ) throws SQLException;

    /**
     * Lock the back-end database using lockName, a shared lock, non-exclusive.
     * @param lockName the lock name to use, must be non-zero and positive, less than {@link Integer#MAX_VALUE}
     * @throws IllegalArgumentException when lockName less than 1 or is {@link Integer#MAX_VALUE}
     * @throws IllegalStateException when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    void lockShared( Long lockName ) throws SQLException;

    /**
     * Unlock the back-end database using lockName that was previously locked.
     * @param lockName the lock name to use, must be non-zero and positive, less than {@link Integer#MAX_VALUE}, and 
     *            have been previously locked with {@link #lockShared(Long)}
     * @throws IllegalArgumentException when lockName less than 1 or is {@link Integer#MAX_VALUE} or was not previously 
     *            locked
     * @throws IllegalStateException when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails
     */

    void unlockShared( Long lockName ) throws SQLException;

    /**
     * Creates an instance for {@link DatabaseType#H2} or {@link DatabaseType#POSTGRESQL}.
     * 
     * @param systemSettings the system settings
     * @param connectionSupplier the connection supplier
     * @return the instance, if recognized
     * @throws UnsupportedOperationException if the configured database type is not recognized
     */

    static DatabaseLockManager from( SystemSettings systemSettings,
                                     Supplier<Connection> connectionSupplier )
    {
        if ( systemSettings.isInMemory() )
        {
            return new DatabaseLockManagerNoop();
        }
        else if ( systemSettings.getDatabaseType() == DatabaseType.POSTGRESQL )
        {
            return new DatabaseLockManagerPostgres( connectionSupplier );
        }
        else if ( systemSettings.getDatabaseType() == DatabaseType.H2 )
        {
            return new DatabaseLockManagerNoop();
        }

        throw new UnsupportedOperationException( "Only supports H2 and PostgreSQL." );
    }
}

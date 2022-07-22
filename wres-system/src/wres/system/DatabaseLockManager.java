package wres.system;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * Manages application-level locks on database objects, exposes lock(), unlock()
 *
 * Re-entrance is disallowed. In other words, it is an error to attempt to
 * acquire the same semantic lock twice before releasing the same lock.
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
     * Lock the back-end database using lockName which is a source id
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE
     * @throws DatabaseLockFailed when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    void lockSource( Long lockName ) throws SQLException;


    /**
     * Unlock the back-end database using lockName that was previously locked
     * using lockSource.
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE, and have been previously
     *                 locked with lock()
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE or was not previously locked
     * @throws DatabaseLockFailed when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails on both attempts.
     */

    void unlockSource( Long lockName ) throws SQLException;


    /**
     * Lock the back-end database using lockName, a shared lock, non-exclusive
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE
     * @throws DatabaseLockFailed when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    void lockExclusive( Long lockName ) throws SQLException;


    /**
     * Unlock the back-end database using lockName that was previously locked
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE, and have been previously
     *                 locked with lock()
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE or was not previously locked
     * @throws IllegalStateException when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails
     */

    void unlockExclusive( Long lockName ) throws SQLException;


    /**
     * Lock the back-end database using lockName, a shared lock, non-exclusive
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE
     * @throws IllegalStateException when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    void lockShared( Long lockName ) throws SQLException;


    /**
     * Unlock the back-end database using lockName that was previously locked
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE, and have been previously
     *                 locked with lock()
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE or was not previously locked
     * @throws IllegalStateException when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails
     */

    void unlockShared( Long lockName ) throws SQLException;


    boolean isSourceLocked( Long lockName ) throws SQLException;


    static DatabaseLockManager from( SystemSettings settings )
    {
        if( settings.isInMemory() )
        {
            return new DatabaseLockManagerNoop();
        }
        else if ( settings.getDatabaseType() == DatabaseType.POSTGRESQL )
        {
            Supplier<Connection> connectionSupplier = new DatabaseConnectionSupplier( settings );
            return new DatabaseLockManagerPostgres( connectionSupplier );
        }
        else if ( settings.getDatabaseType() == DatabaseType.H2 )
        {
            return new DatabaseLockManagerNoop();
        }

        throw new UnsupportedOperationException( "Only supports H2 and PostgreSQL." );
    }
}

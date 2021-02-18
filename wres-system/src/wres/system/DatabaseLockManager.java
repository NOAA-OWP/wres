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
     * Liquibase changes or "clean" or "remove orphans" should use
     * exclusive lock on this. Any and every ingest/evaluation should first get
     * a shared lock on this, except those mentioned above, which should get it
     * exclusively.
     */
    Integer SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME = 1;

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

    void lockSource( Integer lockName ) throws SQLException;


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

    void unlockSource( Integer lockName ) throws SQLException;


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

    void lockExclusive( Integer lockName ) throws SQLException;


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

    void unlockExclusive( Integer lockName ) throws SQLException;


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

    void lockShared( Integer lockName ) throws SQLException;


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

    void unlockShared( Integer lockName ) throws SQLException;


    boolean isSourceLocked( Integer lockName ) throws SQLException;


    static DatabaseLockManager from( SystemSettings settings )
    {
        if ( settings.getDatabaseType()
                     .equalsIgnoreCase( "postgresql" ) )
        {
            Supplier<Connection> connectionSupplier = new DatabaseConnectionSupplier( settings );
            return new DatabaseLockManagerPostgres( connectionSupplier );
        }
        else if ( settings.getDatabaseType()
                          .equalsIgnoreCase( "h2" ) )
        {
            return new DatabaseLockManagerNoop();
        }

        throw new UnsupportedOperationException( "Only supports H2 and PostgreSQL." );
    }
}

package wres.system;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static wres.system.DatabaseLockFailed.Operation.*;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages application-level locks on database objects, exposes lock(), unlock()
 *
 * These are outside of or in addition to the usual locks on tables and rows
 * for queries.
 *
 * If there are different strategies required for locking with one rdbms versus
 * another rdbms, those differences are in different DatabaseLockManager
 * implementations.
 *
 * Exactly one instance of this class is expected to be used by each WRES
 * process and shared as needed.
 *
 * Intended to be Thread-safe.
 *
 * Each semantic lock (caller-provided lock name) is a positive {@link Integer}.
 * The opposite of the Integer is used on the second {@link Connection} managed
 * by this.
 *
 * Re-entrance is disallowed. In other words, it is an error to attempt to
 * acquire the same semantic lock twice before releasing the same lock.
 */

public class DatabaseLockManagerPostgres implements DatabaseLockManager
{
    private static final String MUST_PASS_NAME_OF_PREVIOUSLY_LOCKED_LOCK_NOT = "Must pass name of previously-locked lock, not ";

    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseLockManagerPostgres.class );

    private static final Duration REFRESH_FREQUENCY = Duration.ofSeconds( 1 );

    /**
     * The prefix for a non-source lock signifying either non-destructive
     * changes (when shared) or destructive changes (when exclusive).
     */
    private static final Integer PREFIX = 1;

    /**
     * SQLState codes to treat like connection errors that can be recovered.
     * As of 2020-05-25 these are postgres-specific.
     */
    private static final Set<String> RECOVERABLE_SQLSTATES =
            Set.of( "08000", "08003", "08006", "57P01" );

    /**
     * Ingest data, copy/insert data into existing tables but no delete, only
     * to be got exclusively to mark "ingest in progress"
     */
    private static final Integer INGEST_SOURCE_PREFIX = 2;

    /** Producer that this manager can get new connections from. */
    private final Supplier<Connection> connectionProducer;
    /** Internal coarse (int) exclusive source locks held by this manager */
    private final ConcurrentSkipListSet<Integer> sourceLockNames;
    /** Internal coarse (int) shared locks held by this manager */
    private final ConcurrentSkipListSet<Integer> sharedLockNames;
    /** Internal coarse (int) exclusive locks held by this manager */
    private final ConcurrentSkipListSet<Integer> exclusiveLockNames;

    /** First connection to use for any given semantic lock (left) */
    private Connection connectionOne;

    /** Second connection to use for any given semantic lock (right)*/
    private Connection connectionTwo;

    /**
     * Internal lock to manage access to connectionOne
     * @Guards connectionOne
     */
    private final ReentrantLock lockOne;

    /**
     * Internal lock to manage access to connectionTwo
     * @Guards connectionTwo
     */
    private final ReentrantLock lockTwo;

    private final ScheduledExecutorService connectionMonitorService;

    public DatabaseLockManagerPostgres( Supplier<Connection> connectionProducer )
    {
        LOGGER.debug( "Began construction of lock manager {}.", this );
        this.connectionProducer = connectionProducer;
        this.connectionOne = connectionProducer.get();
        this.connectionTwo = connectionProducer.get();
        this.sourceLockNames = new ConcurrentSkipListSet<>();
        this.sharedLockNames = new ConcurrentSkipListSet<>();
        this.exclusiveLockNames = new ConcurrentSkipListSet<>();
        this.lockOne = new ReentrantLock();
        this.lockTwo = new ReentrantLock();

        ThreadFactory monitorServiceNaming = new BasicThreadFactory.Builder()
                .namingPattern( "DatabaseLockManager %d" )
                .build();

        this.connectionMonitorService = Executors.newScheduledThreadPool( 1, monitorServiceNaming );
        Runnable recurringTask = new RefreshConnectionsTask( this );

        // Use fixed delay instead of rate now that task can sleep per source.
        this.connectionMonitorService.scheduleWithFixedDelay( recurringTask,
                                                              REFRESH_FREQUENCY.getSeconds(),
                                                              REFRESH_FREQUENCY.getSeconds(),
                                                              TimeUnit.SECONDS );

        LOGGER.debug( "Finished construction of lock manager {}.", this );
    }

    /**
     * Shutdown the lock manager.
     * 
     * @throws SecurityException in the absence of permission
     */

    public void shutdown()
    {
        LOGGER.debug( "Shutting down lock manager {}", this );

        List<Runnable> abandoned = this.connectionMonitorService.shutdownNow();

        LOGGER.debug( "Abandoned during shutdown: {}", abandoned );

        // Attempt to clean up any previously-locked-but-not-yet-unlocked locks
        try
        {
            for ( Integer lockName : this.sourceLockNames )
            {
                this.pgUnlockSource( lockName );
            }

            for ( Integer lockName : this.sharedLockNames )
            {
                this.pgUnlockShared( lockName );
            }

            for ( Integer lockName : this.exclusiveLockNames )
            {
                this.pgUnlockExclusive( lockName );
            }
        }
        catch ( SQLException | DatabaseLockFailed e )
        {
            LOGGER.warn( "Unable to unlock remaining database locks.", e );
        }

        if ( !this.sourceLockNames.isEmpty() )
        {
            LOGGER.warn( "Ingest did not clean up by unlocking these sources: {}",
                         this.sourceLockNames );
        }

        if ( !this.sharedLockNames.isEmpty() )
        {
            LOGGER.warn( "WRES did not clean up by unlocking these shared locks: {}",
                         this.sharedLockNames );
        }

        if ( !this.exclusiveLockNames.isEmpty() )
        {
            LOGGER.warn( "WRES did not clean up by unlocking these exclusive locks: {}",
                         this.exclusiveLockNames );
        }


        this.lockOne.lock();

        try
        {
            this.connectionOne.close();
        }
        catch ( SQLException se )
        {
            LOGGER.warn( "Failed to close a connection (1) {}",
                         this.connectionOne, se );
        }
        finally
        {
            this.lockOne.unlock();
        }

        this.lockTwo.lock();

        try
        {
            this.connectionTwo.close();
        }
        catch ( SQLException se )
        {
            LOGGER.warn( "Failed to close a connection (2) {}",
                         this.connectionTwo, se );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        LOGGER.debug( "Successfully shut down lock manager {}", this );
    }

    @Override
    public void lockSource( Long lockName ) throws SQLException
    {
        Integer pgLockName = getIntegerLockNameFromLong( lockName );
        this.pgLockSource( pgLockName );
    }

    @Override
    public void unlockSource( Long lockName ) throws SQLException
    {
        Integer pgLockName = getIntegerLockNameFromLong( lockName );
        this.pgUnlockSource( pgLockName );
    }

    @Override
    public void lockExclusive( Long lockName ) throws SQLException
    {
        Integer pgLockName = getIntegerLockNameFromLong( lockName );
        this.pgLockExclusive( pgLockName );
    }

    @Override
    public void unlockExclusive( Long lockName ) throws SQLException
    {
        Integer pgLockName = getIntegerLockNameFromLong( lockName );
        this.pgUnlockExclusive( pgLockName );
    }

    @Override
    public void lockShared( Long lockName ) throws SQLException
    {
        Integer pgLockName = getIntegerLockNameFromLong( lockName );
        this.pgLockShared( pgLockName );
    }

    @Override
    public void unlockShared( Long lockName ) throws SQLException
    {
        Integer pgLockName = getIntegerLockNameFromLong( lockName );
        this.pgUnlockShared( pgLockName );
    }

    @Override
    public boolean isSourceLocked( Long lockName ) throws SQLException
    {
        Integer pgLockName = getIntegerLockNameFromLong( lockName );
        return this.isPgSourceLocked( pgLockName );
    }

    /**
     * Lock the back-end database using lockName which is a source id
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer#MAX_VALUE}
     * @throws IllegalArgumentException when lockName less than 1 or is
     * MAX_VALUE
     * @throws DatabaseLockFailed when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    private void pgLockSource( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.lockSource( {} ) {}", lockName, this );

        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        boolean added = this.sourceLockNames.add( lockName );

        if ( !added )
        {
            throw new IllegalStateException( "Already had a source lock on " + lockName );
        }

        boolean firstLockSucceeded = false;
        boolean firstLockHadConnectionClosed = false;
        this.lockOne.lock();

        try
        {
            firstLockSucceeded = this.acquireSingleLock( this.connectionOne,
                                                         INGEST_SOURCE_PREFIX,
                                                         lockName,
                                                         true );
        }
        catch ( SQLException se )
        {
            if ( RECOVERABLE_SQLSTATES.contains( se.getSQLState() ) )
            {
                LOGGER.warn( "Lost first connection, should recover soon: {}",
                             se.getMessage() );
                firstLockHadConnectionClosed = true;
            }
            else
            {
                LOGGER.warn( "Unrecoverable SQLState on connection one: {}",
                             se.getSQLState() );
                throw se;
            }
        }
        finally
        {
            this.lockOne.unlock();
        }

        if ( !firstLockSucceeded && !firstLockHadConnectionClosed )
        {
            this.sourceLockNames.remove( lockName );
            throw new DatabaseLockFailed( INGEST_SOURCE_PREFIX,
                                          lockName,
                                          LOCK_EXCLUSIVE );
        }

        boolean secondLockSucceeded = true;
        this.lockTwo.lock();

        try
        {
            secondLockSucceeded = this.acquireSingleLock( this.connectionTwo,
                                                          INGEST_SOURCE_PREFIX,
                                                          -lockName,
                                                          true );
        }
        catch ( SQLException se )
        {
            if ( !firstLockHadConnectionClosed
                 && RECOVERABLE_SQLSTATES.contains( se.getSQLState() ) )
            {
                LOGGER.warn( "Lost second connection, should recover soon: {}",
                             se.getMessage() );
            }
            else
            {
                LOGGER.warn( "Unrecoverable SQLState on connection two: {}",
                             se.getSQLState() );
                throw se;
            }
        }
        finally
        {
            this.lockTwo.unlock();
        }

        if ( !secondLockSucceeded )
        {
            this.sourceLockNames.remove( lockName );
            throw new DatabaseLockFailed( INGEST_SOURCE_PREFIX,
                                          -lockName,
                                          LOCK_EXCLUSIVE );
        }

        LOGGER.trace( "Ended DatabaseLockManager.lockSource( {} ) {}", lockName, this );
    }

    /**
     * Unlock the back-end database using lockName that was previously locked
     * using lockSource.
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer#MAX_VALUE}, and have been previously
     *                 locked with lock()
     * @throws IllegalArgumentException when lockName less than 1 or is
     * {@link Integer#MAX_VALUE} or was not previously locked
     * @throws DatabaseLockFailed when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails on both attempts.
     */
    private void pgUnlockSource( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.unlockSource( {} ) {}", lockName, this );
        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        if ( !this.sourceLockNames.contains( lockName ) )
        {
            throw new IllegalArgumentException( MUST_PASS_NAME_OF_PREVIOUSLY_LOCKED_LOCK_NOT
                                                + lockName);
        }

        boolean firstUnlockSucceeded = false;
        boolean firstUnlockHadConnectionClosed = false;

        this.lockOne.lock();

        try
        {
            firstUnlockSucceeded = this.releaseSingleLock( this.connectionOne,
                                                           INGEST_SOURCE_PREFIX,
                                                           lockName,
                                                           true );
        }
        catch ( SQLException se )
        {
            if ( RECOVERABLE_SQLSTATES.contains( se.getSQLState() ) )
            {
                LOGGER.warn( "Lost first connection, should recover soon: {}",
                             se.getMessage() );
                firstUnlockHadConnectionClosed = true;
            }
            else
            {
                LOGGER.warn( "Unrecoverable SQLState on first connection: {}",
                             se.getSQLState() );
                throw se;
            }
        }
        finally
        {
            this.lockOne.unlock();
        }

        if ( !firstUnlockSucceeded && !firstUnlockHadConnectionClosed )
        {
            throw new DatabaseLockFailed( INGEST_SOURCE_PREFIX,
                                          lockName,
                                          UNLOCK_EXCLUSIVE );
        }

        // If we default to true, will remain true after losing second
        // connection meaning that we will definitely throw an exception when
        // both connections were lost but not when only one was lost.
        boolean secondUnlockSucceeded = true;
        this.lockTwo.lock();

        try
        {
            secondUnlockSucceeded = this.releaseSingleLock( this.connectionTwo,
                                                            INGEST_SOURCE_PREFIX,
                                                            -lockName,
                                                            true );
        }
        catch ( SQLException se )
        {
            if ( !firstUnlockHadConnectionClosed
                 && RECOVERABLE_SQLSTATES.contains( se.getSQLState() ) )
            {
                LOGGER.warn( "Lost second connection, should recover soon: {}",
                             se.getMessage() );
            }
            else
            {
                // Note the if !firstUnlockHadConnectionClosed above, this means
                // that when firstUnlockHadConnectionClosed, we will throw here.
                LOGGER.warn( "Unrecoverable SQLState on second connection: {}",
                             se.getSQLState() );
                throw se;
            }
        }
        finally
        {
            this.lockTwo.unlock();
        }

        if ( !secondUnlockSucceeded )
        {
            throw new DatabaseLockFailed( INGEST_SOURCE_PREFIX,
                                          -lockName,
                                          UNLOCK_EXCLUSIVE );
        }

        boolean removed = this.sourceLockNames.remove( lockName );

        if ( !removed )
        {
            throw new IllegalStateException( "Some other Thread must have called unlockSource with "
                                             + lockName );
        }

        LOGGER.trace( "Ended DatabaseLockManager.unlockSource( {} ) {}", lockName, this );
    }


    /**
     * Lock the back-end database using lockName, a shared lock, non-exclusive
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer#MAX_VALUE}
     * @throws IllegalArgumentException when lockName less than 1 or is
     * {@link Integer#MAX_VALUE}
     * @throws DatabaseLockFailed when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    private void pgLockExclusive( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.lockExclusive( {} ) {}", lockName, this );

        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        if ( this.sharedLockNames.contains( lockName ) )
        {
            throw new IllegalStateException( "A shared lock already exists on " + lockName );
        }

        boolean added = this.exclusiveLockNames.add( lockName );

        if ( !added )
        {
            throw new IllegalStateException( "Already had an exclusive lock on " + lockName );
        }

        boolean firstLockSucceeded;
        this.lockOne.lock();

        try
        {
            firstLockSucceeded = this.acquireSingleLock( this.connectionOne,
                                                         PREFIX,
                                                         lockName,
                                                         true );
        }
        finally
        {
            this.lockOne.unlock();
        }

        if ( !firstLockSucceeded )
        {
            this.exclusiveLockNames.remove( lockName );
            throw new DatabaseLockFailed( PREFIX, lockName, LOCK_EXCLUSIVE );
        }

        boolean secondLockSucceeded;
        this.lockTwo.lock();

        try
        {
            secondLockSucceeded = this.acquireSingleLock( this.connectionTwo,
                                                          PREFIX,
                                                          -lockName,
                                                          true );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        if ( !secondLockSucceeded )
        {
            this.exclusiveLockNames.remove( lockName );
            throw new DatabaseLockFailed( PREFIX, -lockName, LOCK_EXCLUSIVE );
        }

        LOGGER.trace( "Ended DatabaseLockManager.lockExclusive( {} ) {}", lockName, this );
    }


    /**
     * Unlock the back-end database using lockName that was previously locked
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer#MAX_VALUE}, and have been previously
     *                 locked with lock()
     * @throws IllegalArgumentException when lockName less than 1 or is
     * {@link Integer#MAX_VALUE} or was not previously locked
     * @throws IllegalStateException when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails
     */

    private void pgUnlockExclusive( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.unlockExclusive( {} ) {}", lockName, this );
        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        if ( !this.exclusiveLockNames.contains( lockName ) )
        {
            throw new IllegalArgumentException( MUST_PASS_NAME_OF_PREVIOUSLY_LOCKED_LOCK_NOT
                                                + lockName);
        }

        boolean firstUnlockSucceeded;
        this.lockOne.lock();

        try
        {
            firstUnlockSucceeded = this.releaseSingleLock( this.connectionOne,
                                                           PREFIX,
                                                           lockName,
                                                           true );
        }
        finally
        {
            this.lockOne.unlock();
        }

        if ( !firstUnlockSucceeded )
        {
            throw new DatabaseLockFailed( PREFIX, lockName, UNLOCK_EXCLUSIVE );
        }

        boolean secondUnlockSucceeded;
        this.lockTwo.lock();

        try
        {
            secondUnlockSucceeded = this.releaseSingleLock( this.connectionTwo,
                                                            PREFIX,
                                                            -lockName,
                                                            true );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        if ( !secondUnlockSucceeded )
        {
            throw new DatabaseLockFailed( PREFIX, -lockName, UNLOCK_EXCLUSIVE );
        }

        boolean removed = this.exclusiveLockNames.remove( lockName );

        if ( !removed )
        {
            throw new IllegalStateException( "Some other Thread must have called unlockExclusive with "
                                             + lockName );
        }

        LOGGER.trace( "Ended DatabaseLockManager.unlockExclusive( {} ) {}", lockName, this );
    }


    /**
     * Lock the back-end database using lockName, a shared lock, non-exclusive
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer#MAX_VALUE}
     * @throws IllegalArgumentException when lockName less than 1 or is
     * {@link Integer#MAX_VALUE}
     * @throws IllegalStateException when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    private void pgLockShared( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.lockShared( {} ) {}", lockName, this );

        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        boolean added = this.sharedLockNames.add( lockName );

        if ( !added )
        {
            throw new IllegalStateException( "Already had a lock on " + lockName );
        }

        boolean firstLockSucceeded;
        this.lockOne.lock();

        try
        {
            firstLockSucceeded = this.acquireSingleLock( this.connectionOne,
                                                         PREFIX,
                                                         lockName,
                                                         false );
        }
        finally
        {
            this.lockOne.unlock();
        }

        if ( !firstLockSucceeded )
        {
            this.sharedLockNames.remove( lockName );
            throw new DatabaseLockFailed( PREFIX, lockName, LOCK_SHARED );
        }

        boolean secondLockSucceeded;
        this.lockTwo.lock();

        try
        {
            secondLockSucceeded = this.acquireSingleLock( this.connectionTwo,
                                                          PREFIX,
                                                          -lockName,
                                                          false );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        if ( !secondLockSucceeded )
        {
            this.sharedLockNames.remove( lockName );
            throw new DatabaseLockFailed( PREFIX, -lockName, LOCK_SHARED );
        }

        LOGGER.trace( "Ended DatabaseLockManager.lockShared( {} ) {}", lockName, this );
    }


    /**
     * Unlock the back-end database using lockName that was previously locked
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer#MAX_VALUE}, and have been previously
     *                 locked with lock()
     * @throws IllegalArgumentException when lockName less than 1 or is
     * {@link Integer#MAX_VALUE} or was not previously locked
     * @throws IllegalStateException when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails
     */

    private void pgUnlockShared( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.unlockShared( {} ) {}", lockName, this );
        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        if ( !this.sharedLockNames.contains( lockName ) )
        {
            throw new IllegalArgumentException( MUST_PASS_NAME_OF_PREVIOUSLY_LOCKED_LOCK_NOT
                                                + lockName);
        }

        this.lockOne.lock();

        try
        {
            this.releaseSingleLock( this.connectionOne,
                                    PREFIX,
                                    lockName,
                                    false );
        }
        finally
        {
            this.lockOne.unlock();
        }

        this.lockTwo.lock();

        try
        {
            this.releaseSingleLock( this.connectionTwo,
                                    PREFIX,
                                    -lockName,
                                    false );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        boolean removed = this.sharedLockNames.remove( lockName );

        if ( !removed )
        {
            throw new IllegalStateException( "Some other Thread must have called unlock with "
                                             + lockName );
        }

        LOGGER.trace( "Ended DatabaseLockManager.unlockShared( {} ) {}", lockName, this );
    }



    private boolean isPgSourceLocked( Integer lockName ) throws SQLException
    {
        // Locked by this manager?
        // The lock acquisition methods validate against the lock being present in this set of locks
        boolean isLockedInternal = this.sourceLockNames.contains( lockName );
        
        if( isLockedInternal )
        {
            return true;
        }
        
        boolean oneLocked;
        boolean twoLocked;

        // Ensure that within this lock manager, only one Thread touches a
        // given lockName at a time.
        this.lockOne.lock();

        try
        {
            oneLocked = this.isSingleLockHeld( this.connectionOne,
                                               INGEST_SOURCE_PREFIX,
                                               lockName );
        }
        finally
        {
            this.lockOne.unlock();
        }

        this.lockTwo.lock();

        try
        {
            twoLocked = this.isSingleLockHeld( this.connectionTwo,
                                               INGEST_SOURCE_PREFIX,
                                               -lockName );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        // If either lock was held, report it as being locked.
        return oneLocked || twoLocked;
    }

    /**
     * Tests the connections used for locks, if either connection has been lost,
     * re-creates the lost connection and re-acquires the lost locks for that
     * connection.
     * Should be called periodically by a recurring task.
     * Yields when any other Thread holds a lock, returns without doing the work
     * @throws IllegalStateException when a new connection cannot be established
     * @throws SQLException when a semantic lock cannot be translated to db lock
     * @throws DatabaseLockFailed when repeated attempts to acquire a lock fail.
     * @throws InterruptedException when interrupted waiting to retry a lock.
     */

    private void testAndRefresh() throws SQLException, InterruptedException
    {
        final int RETRY_COUNT = 5;
        final int RETRY_MILLIS = 5;
        LOGGER.trace( "Began refreshing connections {} {}", this, Thread.currentThread() );
        boolean isOneWorking;
        boolean isTwoWorking;

        if ( this.lockOne.tryLock() )
        {
            try
            {
                isOneWorking = this.isAlive( connectionOne );

                if ( !isOneWorking )
                {
                    LOGGER.warn( "About to restore a connection to database (1), lost {}",
                                 this.connectionOne );
                    // Because we lost connection one, get a new connection.
                    this.connectionOne = connectionProducer.get();

                    // Because we lost the connection, we lost our locks, restore them.
                    for ( Integer semanticLock : this.exclusiveLockNames )
                    {
                        boolean success = false;

                        for ( int i = 0; i <= RETRY_COUNT && !success; i++ )
                        {
                            success = this.acquireSingleLock( this.connectionOne,
                                                              PREFIX,
                                                              semanticLock,
                                                              true );
                            if ( !success && i != RETRY_COUNT )
                            {
                                LOGGER.warn( "Re-attempting to acquire exclusive lock {} on connection 1 in {}ms.",
                                             semanticLock, RETRY_MILLIS );
                                Thread.sleep( RETRY_MILLIS );
                            }
                        }

                        if ( !success )
                        {
                            throw new DatabaseLockFailed( PREFIX,
                                                          semanticLock,
                                                          LOCK_EXCLUSIVE );
                        }
                    }

                    for ( Integer semanticLock : this.sharedLockNames )
                    {
                        boolean success = false;

                        for ( int i = 0; i <= RETRY_COUNT && !success; i++ )
                        {
                            success = this.acquireSingleLock( this.connectionOne,
                                                              PREFIX,
                                                              semanticLock,
                                                              false );
                            if ( !success && i != RETRY_COUNT )
                            {
                                LOGGER.warn( "Re-attempting to acquire shared lock {} on connection 1 in {}ms.",
                                             semanticLock, RETRY_MILLIS );
                                Thread.sleep( RETRY_MILLIS );
                            }
                        }
                        if ( !success )
                        {
                            throw new DatabaseLockFailed( PREFIX,
                                                          semanticLock,
                                                          LOCK_SHARED );
                        }
                    }

                    for ( Integer semanticLock : this.sourceLockNames )
                    {
                        boolean success = false;

                        for ( int i = 0; i <= RETRY_COUNT && !success; i++ )
                        {
                            success = this.acquireSingleLock( this.connectionOne,
                                                              INGEST_SOURCE_PREFIX,
                                                              semanticLock,
                                                              true );
                            if ( !success && i != RETRY_COUNT )
                            {
                                LOGGER.warn( "Re-attempting to acquire source lock {} on connection 1 in {}ms.",
                                             semanticLock, RETRY_MILLIS );
                                Thread.sleep( RETRY_MILLIS );
                            }
                        }

                        if ( !success )
                        {
                            throw new DatabaseLockFailed( INGEST_SOURCE_PREFIX,
                                                          semanticLock,
                                                          LOCK_EXCLUSIVE );
                        }
                    }
                }
            }
            finally
            {
                this.lockOne.unlock();
            }
        }
        else
        {
            LOGGER.debug( "Yielded to another Thread for connection one. {} {}",
                          this.lockOne, this );
            return;
        }

        if ( this.lockTwo.tryLock() )
        {
            try
            {
                isTwoWorking = this.isAlive( connectionTwo );

                if ( !isTwoWorking )
                {
                    LOGGER.warn( "About to restore a connection to database (2), lost {}",
                                 this.connectionTwo );
                    // Because we lost connection two, get a new connection.
                    this.connectionTwo = connectionProducer.get();

                    // Because we lost the connection, we lost our locks, restore them.
                    for ( Integer semanticLock : this.exclusiveLockNames )
                    {
                        boolean success = false;

                        for ( int i = 0; i <= RETRY_COUNT && !success; i++ )
                        {
                            success = this.acquireSingleLock( this.connectionTwo,
                                                              PREFIX,
                                                              -semanticLock,
                                                              true );
                            if ( !success && i != RETRY_COUNT )
                            {
                                LOGGER.warn( "Re-attempting to acquire exclusive lock {} on connection 2 in {}ms.",
                                             semanticLock, RETRY_MILLIS );
                                Thread.sleep( RETRY_MILLIS );
                            }
                        }
                        if ( !success )
                        {
                            throw new DatabaseLockFailed( PREFIX,
                                                          -semanticLock,
                                                          LOCK_EXCLUSIVE );
                        }
                    }

                    for ( Integer semanticLock : this.sharedLockNames )
                    {
                        boolean success = false;

                        for ( int i = 0; i <= RETRY_COUNT && !success; i++ )
                        {
                            success = this.acquireSingleLock( this.connectionTwo,
                                                              PREFIX,
                                                              -semanticLock,
                                                              false );
                            if ( !success && i != RETRY_COUNT )
                            {
                                LOGGER.warn( "Re-attempting to acquire shared lock {} on connection 2 in {}ms.",
                                             semanticLock, RETRY_MILLIS );
                                Thread.sleep( RETRY_MILLIS );
                            }
                        }
                        if ( !success )
                        {
                            throw new DatabaseLockFailed( PREFIX,
                                                          -semanticLock,
                                                          LOCK_SHARED );
                        }
                    }

                    for ( Integer semanticLock : this.sourceLockNames )
                    {
                        boolean success = false;

                        for ( int i = 0; i <= RETRY_COUNT && !success; i++ )
                        {
                            success = this.acquireSingleLock( this.connectionTwo,
                                                              INGEST_SOURCE_PREFIX,
                                                              -semanticLock,
                                                              true );
                            if ( !success && i != RETRY_COUNT )
                            {
                                LOGGER.warn( "Re-attempting to acquire source lock {} on connection 2 in {}ms.",
                                             semanticLock, RETRY_MILLIS );
                                Thread.sleep( RETRY_MILLIS );
                            }
                        }
                        if ( !success )
                        {
                            throw new DatabaseLockFailed( INGEST_SOURCE_PREFIX,
                                                          -semanticLock,
                                                          LOCK_EXCLUSIVE );
                        }
                    }
                }
            }
            finally
            {
                this.lockTwo.unlock();
            }
        }
        else
        {
            LOGGER.debug( "Yielded to another Thread for connection two. {} {}",
                          this.lockTwo, this );
            return;
        }

        // We hope that this situation rarely occurs which is why we log a
        // message when it does occur. It should be accompanied with at least
        // one more SQLException that propagates and stops the evaluation.
        // (Otherwise the premise of the two-connection setup is intractable.)
        if ( !isOneWorking && !isTwoWorking )
        {
            LOGGER.warn( "Lost two lock-holding database connections at once." );
        }

        LOGGER.trace( "Ended refreshing connections {}", this );
    }


    /**
     * Recurring task that will keep connections alive.
     */
    private static class RefreshConnectionsTask implements Runnable
    {
        private final DatabaseLockManagerPostgres lockManager;

        RefreshConnectionsTask( DatabaseLockManagerPostgres lockManager )
        {
            this.lockManager = lockManager;
        }

        @Override
        public void run()
        {
            try
            {
                this.lockManager.testAndRefresh();
            }
            catch ( SQLException se )
            {
                LOGGER.warn( "Had trouble managing connections.", se );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while managing connections.", ie );
                Thread.currentThread().interrupt();
            }
            catch ( RuntimeException re )
            {
                LOGGER.warn( "Exception while managing connections: ", re );
            }
        }
    }


    /**
     * Tests the aliveness of a given connection
     * @param connection the connection to test
     * @return true if the connection isValid() and not isClosed() and no
     * exceptions occurred when calling isValid and isClosed, false otherwise.
     */

    private boolean isAlive( Connection connection )
    {
        Objects.requireNonNull( connection, "Unexpected null connection" );

        boolean isValid;
        boolean isOpen;

        try
        {
            isValid = connection.isValid( 1 );
        }
        catch ( SQLException se )
        {
            LOGGER.warn( "Connection {} isValid() call did not work.",
                         connection,
                         se );
            return false;
        }

        try
        {
            isOpen = !connection.isClosed();
        }
        catch ( SQLException se )
        {
            LOGGER.warn( "Connection {} isClosed() call did not work.",
                         connection,
                         se );
            return false;
        }

        // In some situations when isValid returns false, the connection may
        // still be open, because isValid returned false only due to timeout
        // instead of due to the connection really being invalid. Test out the
        // connection in this case, logging a message both before and after
        // an invalid query containing a timestamp. The two WARN messages
        // sandwiching this query should have millisecond-precision timestamps
        // and the error log message on the database side (when connection is
        // up) should also have a millisecond-precision timestamp.
        if ( !isValid && isOpen )
        {
            LOGGER.warn( "Connection {} isOpen but isValid returned false.{}{}",
                         connection,
                         " About to send a query that will fail, to cause the ",
                         "error log on database-side to print time of query." );
            String query = "It is now " + Instant.now()
                                                 .atOffset( ZoneOffset.UTC )
                                                 .toString();
            try ( Statement statement = connection.createStatement();
                  ResultSet resultSet = statement.executeQuery( query ) )
            {
                resultSet.next();
            }
            catch ( SQLException se )
            {
                LOGGER.warn( "Finished attempting this invalid query: {}",
                             query, se );
            }
        }

        return isValid && isOpen;
    }


    /**
     * Acquires a lock on the backend database.
     * @param connection the connection to use
     * @param semanticLock the name of the lock to acquire
     * @throws SQLException when communication with the database fails
     * @throws IllegalStateException when acquisition of the lock fails
     */

    private boolean acquireSingleLock( Connection connection,
                                       Integer prefix,
                                       Integer semanticLock,
                                       boolean exclusive )
            throws SQLException
    {
        String pgFunction;

        if ( exclusive )
        {
            pgFunction = "pg_try_advisory_lock";
        }
        else
        {
            pgFunction = "pg_try_advisory_lock_shared";
        }

        // TODO: change this to pg_advisory_lock to avoid IllegalStateException
        // in this process when another process is using isSingleLockHeld. That
        // or use N retries.
        final String TRY_LOCK_SCRIPT = "SELECT " + pgFunction + "( "
                                       + prefix + ", " + semanticLock
                                       + " )";

        try ( Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery( TRY_LOCK_SCRIPT ) )
        {
            resultSet.next();
            return resultSet.getBoolean( 1 );
        }
    }


    /**
     * Releases a lock on the backend database.
     * Will be called twice by the unlock() method, once for each connection.
     * @param connection the connection to use
     * @param prefix The group of locks we are interested in.
     * @param semanticLock the name of the lock to release
     * @throws SQLException when communication with the database fails
     * @throws IllegalStateException when release of the lock fails
     */

    private boolean releaseSingleLock( Connection connection,
                                       Integer prefix,
                                       Integer semanticLock,
                                       boolean exclusive )
            throws SQLException
    {
        String pgFunction;

        if ( exclusive )
        {
            pgFunction = "pg_advisory_unlock";
        }
        else
        {
            pgFunction = "pg_advisory_unlock_shared";
        }

        final String RELEASE_LOCK_SCRIPT = "SELECT " + pgFunction + "( "
                                           + prefix + ", " + semanticLock
                                           + " )";

        try ( Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery( RELEASE_LOCK_SCRIPT ) )
        {
            resultSet.next();
            return resultSet.getBoolean( 1 );
        }
    }


    /**
     * Find out if any task is holding a lock on a given lock name by attempting
     * to gain an exclusive lock on it then releasing it if it was acquired and
     * then returning true if the lock was acquired.
     * @param connection The connection to use to discover lock status.
     * @param prefix The group of locks we are interested in.
     * @param semanticLock The lock name to discover status on.
     * @return true when a task in- or out-of-process holds this lock, false
     *         otherwise.
     * @throws SQLException When communication with the database fails.
     */
    private boolean isSingleLockHeld( Connection connection,
                                      Integer prefix,
                                      Integer semanticLock )
            throws SQLException
    {
        boolean successfullyLocked = this.acquireSingleLock( connection,
                                                             prefix,
                                                             semanticLock,
                                                             true );

        // Unlock if it was successfully acquired.
        if ( successfullyLocked )
        {
            boolean successfullyUnlocked = this.releaseSingleLock( connection,
                                                                   prefix,
                                                                   semanticLock,
                                                                   true );
            if ( !successfullyUnlocked )
            {
                throw new IllegalStateException( "Unable to unlock after acquiring lock: "
                                                 + prefix + ", " + semanticLock );
            }
        }

        // If we were able to successfully lock, the lock was NOT held.
        return !successfullyLocked;
    }

    static Integer getIntegerLockNameFromLong( Long id )
    {
        return id.intValue() & Integer.MAX_VALUE;
    }
}

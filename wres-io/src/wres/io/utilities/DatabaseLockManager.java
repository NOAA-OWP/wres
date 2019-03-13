package wres.io.utilities;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages application-level locks on database objects, exposes lock(), unlock()
 *
 * These are outside of or in addition to the usual locks on tables and rows
 * for queries.
 *
 * If there are different strategies required for locking with one rdbms versus
 * another rdbms, those differences can be encapsulated in this class.
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

class DatabaseLockManager
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger( DatabaseLockManager.class );

    private static final Duration REFRESH_FREQUENCY = Duration.ofSeconds( 5 );

    /** Producer that this manager can get new connections from. */
    private final Supplier<Connection> connectionProducer;
    /** Semantic locks held by this manager */
    private final ConcurrentSkipListSet<Integer> lockNames;

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


    DatabaseLockManager( Supplier<Connection> connectionProducer )
    {
        LOGGER.debug( "Began construction of lock manager {}.", this );
        this.connectionProducer = connectionProducer;
        this.connectionOne = connectionProducer.get();
        this.connectionTwo = connectionProducer.get();
        this.lockNames = new ConcurrentSkipListSet<>();
        this.lockOne = new ReentrantLock();
        this.lockTwo = new ReentrantLock();

        ThreadFactory monitorService = runnable -> new Thread( runnable, "Database connection monitoring service" );
        this.connectionMonitorService = Executors.newScheduledThreadPool( 1, monitorService );
        Runnable recurringTask = new RefreshConnectionsTask( this );
        this.connectionMonitorService.scheduleAtFixedRate( recurringTask,
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
    
    void shutdown()
    {
        LOGGER.debug( "Shutting down lock manager {}", this );
        
        this.connectionMonitorService.shutdown();
    }
    
    /**
     * Lock the back-end database using lockName
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE
     * @throws IllegalArgumentException when lockName < 1 or is MAX_VALUE
     * @throws IllegalStateException when db reports lock acquisition failed
     * @throws IllegalStateException when lock already was acquired by any Thread
     * @throws SQLException when database communication fails
     */

    void lock( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.lock( {} ) {}", lockName, this );

        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        boolean added = this.lockNames.add( lockName );

        if ( !added )
        {
            throw new IllegalStateException( "Already had a lock on " + lockName );
        }

        this.lockOne.lock();

        try
        {
            this.acquireSingleLock( this.connectionOne, lockName );
        }
        finally
        {
            this.lockOne.unlock();
        }

        this.lockTwo.lock();

        try
        {
            this.acquireSingleLock( this.connectionTwo, -lockName );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        LOGGER.trace( "Ended DatabaseLockManager.lock( {} ) {}", lockName, this );
    }

    /**
     * Unlock the back-end database using lockName that was previously locked
     * @param lockName the lock name to use, must be non-zero and positive, less
     *                 than {@link Integer}.MAX_VALUE, and have been previously
     *                 locked with lock()
     * @throws IllegalArgumentException when lockName < 1 or is MAX_VALUE or was
     * not previously locked
     * @throws IllegalStateException when db reports lock release failed
     * @throws IllegalStateException when unlock was called twice simultaneously
     * @throws SQLException when database communication fails
     */
    void unlock( Integer lockName ) throws SQLException
    {
        LOGGER.trace( "Began DatabaseLockManager.unlock( {} ) {}", lockName, this );
        if ( lockName < 1 || lockName.equals( Integer.MAX_VALUE ) )
        {
            throw new IllegalArgumentException( "Must pass non-zero, non-max int, not "
                                                + lockName );
        }

        if ( !this.lockNames.contains( lockName ) )
        {
            throw new IllegalArgumentException( "Must pass name of previously-locked lock, not"
                                                + lockName);
        }

        this.lockOne.lock();

        try
        {
            this.releaseSingleLock( this.connectionOne, lockName );
        }
        finally
        {
            this.lockOne.unlock();
        }

        this.lockTwo.lock();

        try
        {
            this.releaseSingleLock( this.connectionTwo, -lockName );
        }
        finally
        {
            this.lockTwo.unlock();
        }

        boolean removed = this.lockNames.remove( lockName );

        if ( !removed )
        {
            throw new IllegalStateException( "Some other Thread must have called unlock with "
                                             + lockName );
        }

        LOGGER.trace( "Ended DatabaseLockManager.unlock( {} ) {}", lockName, this );
    }


    /**
     * Tests the connections used for locks, if either connection has been lost,
     * re-creates the lost connection and re-acquires the lost locks for that
     * connection.
     * Should be called periodically by a recurring task.
     * Yields when any other Thread holds a lock, returns without doing the work
     * @throws IllegalStateException when a new connection cannot be established
     * @throws SQLException when a semantic lock cannot be translated to db lock
     */

    private void testAndRefresh() throws SQLException
    {
        LOGGER.trace( "Began refreshing connections {}", this );
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
                    for ( Integer semanticLock : this.lockNames )
                    {
                        this.acquireSingleLock( this.connectionOne,
                                                semanticLock );
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
                    for ( Integer semanticLock : this.lockNames )
                    {
                        this.acquireSingleLock( this.connectionTwo,
                                                -semanticLock );
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
        private final DatabaseLockManager lockManager;

        RefreshConnectionsTask( DatabaseLockManager lockManager )
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
            LOGGER.warn( "Connection {} isValid() call did not work.", se );
            return false;
        }

        try
        {
            isOpen = !connection.isClosed();
        }
        catch ( SQLException se )
        {
            LOGGER.warn( "Connection {} isClosed() call did not work.", se );
            return false;
        }

        return isValid && isOpen;
    }


    /**
     * Acquires a lock on the backend database.
     * @param connection the connection to use
     * @param semanticLock the name of the lock to acquire
     * @throws SQLException when communication with the database fails
     */

    private void acquireSingleLock( Connection connection,
                                    Integer semanticLock )
            throws SQLException
    {
        final String TRY_LOCK_SCRIPT = "SELECT pg_try_advisory_lock( "
                                       + semanticLock
                                       + " )";

        try ( Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery( TRY_LOCK_SCRIPT ) )
        {
            while ( resultSet.next() )
            {
                boolean successfullyLocked = resultSet.getBoolean( 1 );

                if ( !successfullyLocked )
                {
                    throw new IllegalStateException( "Could not lock using "
                                                     + semanticLock );
                }
            }
        }
    }


    /**
     * Releases a lock on the backend database.
     * Will be called twice by the unlock() method, once for each connection.
     * @param connection the connection to use
     * @param semanticLock the name of the lock to release
     * @throws SQLException when communication with the database fails
     */

    private void releaseSingleLock( Connection connection,
                                    Integer semanticLock )
            throws SQLException
    {
        final String RELEASE_LOCK_SCRIPT = "SELECT pg_advisory_unlock( "
                                           + semanticLock
                                           + " )";

        try ( Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery( RELEASE_LOCK_SCRIPT ) )
        {
            while ( resultSet.next() )
            {
                boolean successfullyUnlocked = resultSet.getBoolean( 1 );

                if ( !successfullyUnlocked )
                {
                    throw new IllegalStateException( "Could not unlock using "
                                                     + semanticLock );
                }
            }
        }
    }
}

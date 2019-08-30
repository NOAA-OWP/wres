package wres.io.reading;


import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceCompletedDetails;
import wres.system.DatabaseLockManager;

/**
 * Allows an ingester/reader to mark an ingest complete for a given source.
 */
public class SourceCompleter
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SourceCompleter.class );
    private static final Duration PATIENCE_LEVEL = Duration.ofSeconds( 30 );
    private final int sourceId;
    private final DatabaseLockManager lockManager;

    /**
     * @param sourceId the source to work with
     * @param lockManager the lock manager to use for signals in the db
     */

    public SourceCompleter( int sourceId,
                            DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( lockManager );
        this.sourceId = sourceId;
        this.lockManager = lockManager;
    }


    /**
     * Mark the given source completed because the caller was in charge of
     * ingest and needs to mark it so.
     *
     * Ensure that ingest of a given sourceId is complete, either by verifying
     * that another task has finished it or by finishing it right here and now.
     *
     * Due to #64922 (empty WRDS AHPS data sources) and #65049 (empty CSV
     * data sources), this class tolerates an empty Set of latches and logs a
     * warning (prior behavior was to throw IllegalArgumentException).
     *
     * @param latches the latches to use to coordinate with other ingest tasks
     * @throws IngestException when unable to mark the source complete
     * @throws NullPointerException when latches is null
     */

    public void complete( Set<Pair<CountDownLatch,CountDownLatch>> latches )
            throws IngestException
    {
        // Make sure the ingest is actually complete by sending
        // a signal that we sit and await the ingest of values prior to
        // marking them complete.
        for ( Pair<CountDownLatch, CountDownLatch> latchPair : latches )
        {
            // Say "I am about to sit here and wait, y'all..."
            latchPair.getLeft()
                     .countDown();
        }

        if ( !latches.isEmpty() )
        {
            try
            {
                Duration eachWait = Duration.ofMillis( 1 );

                for ( Pair<CountDownLatch, CountDownLatch> latchPair : latches )
                {
                    // Wait a moment for another task to save my data before
                    // doing it myself.
                    boolean dataFinished = latchPair.getRight()
                                                    .await( eachWait.toMillis(),
                                                            TimeUnit.MILLISECONDS );
                    if ( !dataFinished )
                    {
                        LOGGER.debug( "Sick of waiting for another task, saving data myself! {}, {}",
                                      this.sourceId, latchPair );
                        boolean thisFlushed = IngestedValues.flush( latchPair );

                        // It is still necessary to double-check that the data has
                        // actually been written, because even if we call flush(),
                        // there is no guarantee that this was the task that wrote.
                        // However, because we called flush, we or someone must be
                        // doing the write at this point. Wait indefinitely for it.
                        // On the other hand, if the other task died while
                        // attempting the copy, we cannot sit here and wait forever.
                        // If we truly completed it, await() call would return
                        // immediately. If we did not successfully complete, then we
                        // should be ready to give up after a time to break
                        // deadlock.
                        if ( !thisFlushed )
                        {
                            boolean done = latchPair.getRight()
                                                    .await( PATIENCE_LEVEL.toMillis(),
                                                            TimeUnit.MILLISECONDS );

                            // This is uncomfortable for sure, and a better way
                            // should be found than making an assumption that the
                            // other task failed. The thing we are working around is
                            // that get() may be called on this task prior to get()
                            // on the task responsible for marking completed, while
                            // that other task has had an exception that does not
                            // propagate.
                            if ( !done )
                            {
                                throw new IngestException(
                                        "Another task did not "
                                        + "ingest and complete "
                                        + latchPair
                                        + " within "
                                        + PATIENCE_LEVEL
                                        + ", therefore assuming "
                                        + "it failed." );
                            }
                        }
                    }
                }
            }
            catch ( InterruptedException ie )
            {
                String message =
                        "Interrupted while waiting for another task to ingest data for source "
                        + this.sourceId + ".";
                LOGGER.warn( message, ie );
                Thread.currentThread().interrupt();
                // Additionally throw exception to ensure we don't accidentally mark
                // this source as completed a few lines down.
                throw new IngestException( message, ie );
            }
        }
        else
        {
            LOGGER.warn( "A data source with no data may have been found. Please check your dataset. (Technical info: source_id={})",
                         this.sourceId );
        }

        // This is used to avoid throwing an exception from finally
        Exception exceptionDuringUnlock = null;

        SourceCompletedDetails completedDetails =
                new SourceCompletedDetails( this.sourceId );

        try
        {
            completedDetails.markCompleted();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Successfully marked source {} as completed.",
                              this.sourceId );
            }
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to mark source "
                                       + this.sourceId
                                       + " as completed.", se );
        }
        finally
        {
            try
            {
                lockManager.unlockSource( this.sourceId );
            }
            catch ( SQLException se )
            {
                exceptionDuringUnlock = se;
            }
        }

        if ( exceptionDuringUnlock != null )
        {
            throw new IngestException( "Failed to unflag source "
                                       + this.sourceId
                                       + " as currently ingesting.",
                                       exceptionDuringUnlock );
        }
    }
}

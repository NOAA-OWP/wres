package wres.io.ingesting.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.DefaultDataProvider;
import wres.datamodel.DataProvider;
import wres.io.database.Database;
import wres.io.database.DatabaseOperations;
import wres.io.ingesting.IngestException;
import wres.system.SystemSettings;

/**
 * Facilitates a shared location for copying forecast timeseries values to
 * the database
 */
public final class IngestedValues
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestedValues.class );

    private IngestedValues()
    {
    }

    // Key = partition name, i.e. "partitions.forecastvalue_lead_0"
    // Value = List of values to save to the partition
    private static final ConcurrentMap<String, DefaultDataProvider> VALUES_TO_SAVE = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Pair<CountDownLatch, CountDownLatch>> VALUES_SAVED_LATCHES =
            new ConcurrentHashMap<>();

    /** Guards VALUES_TO_SAVE and VALUES_SAVED_LATCHES and DataBuilders */
    private static final Object VALUES_TO_SAVE_LOCK = new Object();

    private static final String[] TIMESERIES_COLUMN_NAMES = { "timeseries_id", "lead", "series_value" };

    private static final String TABLE_NAME = "wres.TimeSeriesValue";

    /**
     * Stores a time series value so that it may be copied to the database,
     * potentially later, but potentially in this Thread, especially if some
     * other Thread awaits it.
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param timeSeriesID The ID of the time series that the value belongs to
     * @param lead The lead time for the value
     * @param value The value itself
     * @return A synchronizer used to signal "waiting" left, "completed" right.
     * @throws IngestException when the proper partition could not be retrieved
     *                         or when the ingest fails.
     */
    public static Pair<CountDownLatch, CountDownLatch> addTimeSeriesValue( SystemSettings systemSettings,
                                                                           Database database,
                                                                           long timeSeriesID,
                                                                           int lead,
                                                                           Double value )
            throws IngestException
    {
        DefaultDataProvider freshDefaultDataProvider = DefaultDataProvider.with( IngestedValues.TIMESERIES_COLUMN_NAMES );

        // The data builder to add to, whether existing or fresh.
        DefaultDataProvider defaultDataProviderToUse;

        // The data builder removed from the collection, to be ingested.
        DefaultDataProvider removedDefaultDataProvider = null;

        // After copy has completed, this class will count down wasSavedLatch
        // This acts as a signal to callers that their data was saved.
        // When a task needs notification that its data was saved, it signals
        // by counting down taskWaitingLatch, which tells those tasks calling
        // this function to save the data earlier than it would have otherwise.
        CountDownLatch taskWaitingLatch = new CountDownLatch( 1 );
        CountDownLatch wasSavedLatch = new CountDownLatch( 1 );
        Pair<CountDownLatch, CountDownLatch> freshLatches = Pair.of( taskWaitingLatch,
                                                                     wasSavedLatch );

        // The existing latches to be got from the collection when put fails.
        Pair<CountDownLatch, CountDownLatch> latchesToUse;

        // When save is needed, removedLatches will be set.
        Pair<CountDownLatch, CountDownLatch> removedLatches = null;

        boolean doSave = false;

        // To avoid holding the VALUES_TO_SAVE_LOCK too long: after discovering
        // that the save needs to happen, set up new latches, then after
        // releasing the lock, do the save synchronously in this Thread rather
        // than in a Thread off to the side.
        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            // Add a list for the values if it isn't present
            defaultDataProviderToUse = VALUES_TO_SAVE.putIfAbsent( TABLE_NAME,
                                                                   freshDefaultDataProvider );

            // When putIfAbsent returns null, it means it successfully put.
            if ( defaultDataProviderToUse == null )
            {
                defaultDataProviderToUse = freshDefaultDataProvider;
            }

            // Add the values to the list for the partition
            defaultDataProviderToUse.addRow( timeSeriesID, lead, value );

            // Add latches for the values if not present
            latchesToUse = VALUES_SAVED_LATCHES.putIfAbsent( TABLE_NAME,
                                                             freshLatches );

            // When putIfAbsent returns null, it means it successfully put.
            if ( latchesToUse == null )
            {
                latchesToUse = freshLatches;
            }

            int rowCount = defaultDataProviderToUse.getRowCount();
            int maximumCount = systemSettings.getMaximumCopies();

            // If the maximum number of values to copy has been reached, copy the
            // values
            if ( rowCount >= maximumCount )
            {
                LOGGER.trace( "Row count for partition {} is {}, larger than system setting {}",
                              TABLE_NAME, rowCount, maximumCount );
                doSave = true;
            }

            // Can skip this step when row count condition already reached, or
            // when we are using a fresh latch, which is true when latchesToUse
            // is the same as freshLatches.
            if ( !latchesToUse.equals( freshLatches ) )
            {
                LOGGER.trace( "An existing latches object {} was found for partition {}, looking for waiting tasks.",
                              latchesToUse, TABLE_NAME );
                try
                {
                    // We could use getCount() <= 0, however, the documentation
                    // indicates that getCount() is for debugging, not control.
                    if ( latchesToUse.getLeft()
                                     .await( 0, TimeUnit.MICROSECONDS ) )
                    {
                        LOGGER.debug(
                                "At least one task signaled it is waiting for ingest on {}, will ingest {} rows to {}.",
                                latchesToUse,
                                rowCount,
                                TABLE_NAME );
                        doSave = true;
                    }
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.debug( "Interrupted while awaiting signal from ingester",
                                  ie );
                    Thread.currentThread().interrupt();
                }
            }

            if ( doSave )
            {
                removedLatches = VALUES_SAVED_LATCHES.remove( TABLE_NAME );
                removedDefaultDataProvider = VALUES_TO_SAVE.remove( TABLE_NAME );
                // It is understood that another Thread will put fresh values.
            }
        }

        // Save has been requested either from row count maximum or from some
        // other task waiting around for the data to be inserted.
        // It should be safe do this outside the synchronized block because
        // these objects will not be visible to Threads that were waiting to
        // enter the above synchronized block.
        if ( doSave )
        {
            LOGGER.debug( "Attempting to ingest values for {} to {}",
                          removedLatches, TABLE_NAME );

            try
            {
                // Do copy synchronously here. Maybe modify other classes such
                // that synchronous copy is simpler? All that the asynchronous
                // copy does here is slow things down because Thread A awaits
                // Thread B which awaits Thread C to complete this ingest, where
                // it might be better to have Thread B complete the ingest and
                // leave Thread C to do other things. Even better might be to
                // let Thread A do the ingest since it is the one waiting.
                DataProvider provider = removedDefaultDataProvider.build();
                IngestedValues.copy( provider, database, TABLE_NAME );
            }
            catch ( IngestException ce )
            {
                throw new IngestException( "Ingest of values to "
                                           + TABLE_NAME + " failed.", ce );
            }

            // Now that data has been copied, the associated latch should be
            // counted down to signal to tasks waiting to mark data as
            // completely ingested that they may safely do so.
            removedLatches.getRight()
                          .countDown();

            // Because ingest just happened for this partitionName and the
            // passed-in data, the caller need not wait anymore, (signified with
            // already-released latches).
            return Pair.of( new CountDownLatch( 0 ), new CountDownLatch( 0 ) );
        }
        else
        {
            LOGGER.trace( "Did not ingest values for partition {}, returning {}",
                          TABLE_NAME, latchesToUse );
            // Because ingest did not happen for this partitionName, return the
            // latches associated with it.
            return latchesToUse;
        }
    }

    /**
     * Call this when you are an ingester waiting to mark "completed" but no
     * other Thread has helped you out.
     *
     * @param database The database to use.
     * @param synchronizer the handle returned by this class to the ingester,
     *                     representing a superset of data the ingester sent.
     */
    static boolean flush( Database database,
                          Pair<CountDownLatch, CountDownLatch> synchronizer )
            throws IngestException
    {
        LOGGER.trace( "Began flush for synchronizer {}...", synchronizer );

        // The data builder removed from the collection, to be ingested.
        DefaultDataProvider removedDefaultDataProvider = null;

        // When save is needed, removedLatches will be set.
        Pair<CountDownLatch, CountDownLatch> removedLatches = null;

        String tableName = null;

        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            for ( Map.Entry<String, Pair<CountDownLatch, CountDownLatch>> latchPair : VALUES_SAVED_LATCHES.entrySet() )
            {
                // "synchronizer" was an arg to this method
                if ( latchPair.getValue().equals( synchronizer ) )
                {
                    tableName = latchPair.getKey();
                }
            }

            // This means it was found, remove associated builder, latches.
            if ( tableName != null )
            {
                removedLatches = VALUES_SAVED_LATCHES.remove( tableName );
                removedDefaultDataProvider = VALUES_TO_SAVE.remove( tableName );
            }
        }

        if ( tableName == null )
        {
            LOGGER.debug(
                    "Unable to find the synchronizer {} in the current lists (another task is saving, no need to flush here)",
                    synchronizer );
            return false;
        }

        // It should be safe do this outside the synchronized block because
        // these objects will not be visible to Threads that were waiting to
        // enter the above synchronized block.
        LOGGER.trace( "Attempting to flush values for partition {} with {}",
                      tableName, removedDefaultDataProvider );

        try
        {
            // Do copy synchronously here. Maybe modify other classes such
            // that synchronous copy is simpler? All that the asynchronous
            // copy does here is slow things down because Thread A awaits
            // Thread B which awaits Thread C to complete this ingest, where
            // it might be better to have Thread B complete the ingest and
            // leave Thread C to do other things. Even better might be to
            // let Thread A do the ingest since it is the one waiting.
            DataProvider provider = removedDefaultDataProvider.build();
            IngestedValues.copy( provider, database, tableName );
        }
        catch ( IngestException ce )
        {
            throw new IngestException( "Ingest of values to "
                                       + tableName + " failed.", ce );
        }

        // If these latches are not the same as what was passed, something went
        // wrong and the bug needs to be fixed.
        if ( !removedLatches.equals( synchronizer ) )
        {
            throw new IllegalStateException( "Issue when saving data: "
                                             + synchronizer + " was passed but "
                                             + removedLatches + " was removed." );
        }

        // Now that data has been copied, the associated latch should be
        // counted down to signal to tasks waiting to mark data as
        // completely ingested that they may safely do so.
        removedLatches.getRight()
                      .countDown();
        LOGGER.trace( "Completed flush for {}", synchronizer );

        return true;
    }

    /**
     * Copies the data within the provider from the current row through the last row into the schema and table
     * <br>
     * The position of the provider will be at the end of the dataset after function completion
     * @param database The database to use
     * @param table Fully qualified table name to copy data into
     * @throws IngestException When the copy fails.
     */
    private static void copy( DataProvider provider, Database database, final String table )
    {
        List<String> columnNames = provider.getColumnNames();
        List<String[]> values = new ArrayList<>();
        boolean[] charColumns = new boolean[columnNames.size()];

        while ( provider.next() )
        {
            String[] row = new String[columnNames.size()];

            for ( int col = 0; col < columnNames.size(); col++ )
            {
                String representation = provider.toString( columnNames.get( col ) );
                row[col] = representation;
            }

            values.add( row );
        }

        // Until we can figure out how to get exceptions to propagate from
        // submitting to the Database executor, run synchronously in caller's
        // Thread.
        DatabaseOperations.insertIntoDatabase( database,
                                               table,
                                               columnNames,
                                               values,
                                               charColumns );
    }

}

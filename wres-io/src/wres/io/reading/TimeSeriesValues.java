package wres.io.reading;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.TimeSeries;
import wres.io.utilities.DataBuilder;
import wres.system.SystemSettings;

/**
 * Facilitates a shared location for copying forecast timeseries values to
 * the database
 */
public final class TimeSeriesValues
{
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesValues.class );

    private static final TimeSeriesValues ourInstance = new TimeSeriesValues();

    public static TimeSeriesValues getInstance()
    {
        return ourInstance;
    }

    private TimeSeriesValues()
    {
    }

    // Key = partition name, i.e. "partitions.forecastvalue_lead_0"
    // Value = List of values to save to the partition
    private static final ConcurrentMap<String, DataBuilder> VALUES_TO_SAVE = new ConcurrentHashMap<>(  );

    /** Guards VALUES_TO_SAVE */
    private static final Object VALUES_TO_SAVE_LOCK = new Object();

    private static final String[] COLUMN_NAMES = {"timeseries_id", "lead", "series_value"};

    /**
     * Stores a time series value so that it may be copied to the database later
     * @param timeSeriesID The ID of the time series that the value belongs to
     * @param lead The lead time for the value
     * @param value The value itself
     * @throws SQLException Thrown if the name of the proper partition could not
     * be retrieved.
     */
    public static void add(final int timeSeriesID, final int lead, final Double value)
            throws SQLException
    {
        String partitionName = TimeSeries.getTimeSeriesValuePartition( lead );

        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            // Add a list for the values if it isn't present
            VALUES_TO_SAVE.putIfAbsent( partitionName, DataBuilder.with( TimeSeriesValues.COLUMN_NAMES ));

            // Add the values to the list for the partition
            VALUES_TO_SAVE.get( partitionName ).addRow( timeSeriesID, lead, value );

            // If the maximum number of values to copy has been reached, copy the
            // values
            if ( VALUES_TO_SAVE.get( partitionName ).getRowCount() >= SystemSettings.getMaximumCopies())
            {
                TimeSeriesValues.copy( partitionName );
            }
        }
    }

    /**
     * Creates and executes a task used to copy the values for a partition to
     * the database
     * @param partitionName The name of the partition whose values need to be
     *                      saved
     */
    private static Future<?> copy(String partitionName)
    {
        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            Future<?> task = VALUES_TO_SAVE.get( partitionName ).build().copy( partitionName );
            VALUES_TO_SAVE.get( partitionName ).reset();
            return task;
        }
    }

    /**
     * Send all values across all stored partitions to the database
     * 
     * @throws IOException if the values cannot be saved to the database
     */
    public static void complete() throws IOException
    {
        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            List<Future<?>> tasks = new ArrayList<>();
            for (String partitionName : VALUES_TO_SAVE.keySet())
            {
                tasks.add(TimeSeriesValues.copy( partitionName ));
            }

            for (Future<?> task : tasks)
            {
                try
                {
                    task.get();
                }
                catch ( InterruptedException e )
                {
                    LOGGER.warn("Thread Interrupted.");
                    Thread.currentThread().interrupt();
                }
                catch ( ExecutionException e )
                {
                    throw new IOException( "Error occurred while attempting to save ingested values.", e );
                }
            }
        }
    }
}

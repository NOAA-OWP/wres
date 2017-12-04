package wres.io.reading;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import wres.io.concurrency.CopyExecutor;
import wres.io.config.SystemSettings;
import wres.io.data.details.TimeSeries;
import wres.io.utilities.Database;
import wres.util.Strings;

/**
 * Facilitates a shared location for copying forecast timeseries values to
 * the database
 */
public final class TimeSeriesValues
{
    private static final TimeSeriesValues ourInstance = new TimeSeriesValues();

    public static TimeSeriesValues getInstance()
    {
        return ourInstance;
    }

    private TimeSeriesValues()
    {
    }

    private static final String NEWLINE = System.lineSeparator();
    private static final String DELIMITER = "|";

    // Key = partition name, i.e. "partitions.forecastvalue_lead_0"
    // Value = List of values to save to the partition
    private static final ConcurrentMap<String, List<TimeSeriesValue>> VALUES_TO_SAVE = new ConcurrentHashMap<>(  );

    // Maps the name of a partition to its table definition. Prevents
    // the system from having to concatentate strings hundreds of thousands,
    // if not millions of times
    private static final ConcurrentMap<String, String> TABLE_DEFINITIONS = new ConcurrentHashMap<>(  );

    /** @Guards VALUES_TO_SAVE */
    private static final Object VALUES_TO_SAVE_LOCK = new Object();

    // The definition of the columns that will have values copied to
    private static final String COLUMN_DEFINITION =  " (timeseries_id, lead, forecasted_value)";

    /**
     * Creates and retrieces the definition for a table
     * @param partitionName The name of the partition whose table definition is
     *                      needed
     * @return The entire definition of the table to copy values to
     */
    private static String getTableDefinition(String partitionName)
    {
        if (!TABLE_DEFINITIONS.containsKey( partitionName ))
        {
            TABLE_DEFINITIONS.putIfAbsent( partitionName,
                                           partitionName + COLUMN_DEFINITION );
        }
        return TABLE_DEFINITIONS.get( partitionName );
    }

    /**
     * Stores a time series value so that it may be copied to the database later
     * @param timeSeriesID The ID of the time series that the value belongs to
     * @param lead The lead time for the value
     * @param value The value itself
     * @throws SQLException Thrown if the name of the proper partition could not
     * be retrieved.
     */
    static void add(int timeSeriesID, int lead, String value)
            throws SQLException
    {
        String partitionName = TimeSeries.getForecastValueParitionName( lead );

        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            // Add a list for the values if it isn't present
            VALUES_TO_SAVE.putIfAbsent( partitionName, new LinkedList<>(  ) );

            // Add the values to the list for the partition
            VALUES_TO_SAVE.get( partitionName ).add( new TimeSeriesValue( timeSeriesID, lead, value ) );

            // If the maximum number of values to copy has been reached, copy the
            // values
            if ( VALUES_TO_SAVE.get( partitionName ).size() >= SystemSettings.getMaximumCopies())
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
    private static void copy(String partitionName)
    {
        StringBuilder values = new StringBuilder(  );

        for (TimeSeriesValue value : VALUES_TO_SAVE.get(partitionName))
        {
            values.append( value.toString() );
        }

        CopyExecutor copier = new CopyExecutor( TimeSeriesValues.getTableDefinition( partitionName ),
                                                values.toString(),
                                                DELIMITER );
        Database.ingest(copier);

        // Replace the old list of values with a new one
        VALUES_TO_SAVE.put( partitionName, new LinkedList<>(  ) );
    }

    /**
     * Send all values across all stored partitions to the database
     */
    public static void complete()
    {
        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            for (String partitionName : VALUES_TO_SAVE.keySet())
            {
                TimeSeriesValues.copy( partitionName );
            }
        }
    }

    /**
     * Represents a single value to save to the database
     */
    private static class TimeSeriesValue
    {
        public TimeSeriesValue(int timeSeriesID, int lead, String value)
        {
            this.timeSeriesID = timeSeriesID;
            this.lead = lead;

            if (!Strings.hasValue(value))
            {
                value = "\\N";
            }
            this.value = value;
        }

        /**
         * @return The format of the value to add to the copy script
         */
        @Override
        public String toString()
        {
            return String.valueOf( this.timeSeriesID ) + DELIMITER +
                   String.valueOf( this.lead ) + DELIMITER +
                   String.valueOf( this.value ) + NEWLINE;
        }

        private final int timeSeriesID;
        private final int lead;
        private final String value;
    }
}

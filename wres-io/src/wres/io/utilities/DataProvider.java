package wres.io.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.ResultSet;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import wres.datamodel.MissingValues;

/**
 * <p>Provides access and operations on tabular data.
 * 
 * <p>Column names used as input to getter methods are case insensitive, much like a {@link ResultSet}.
 */
public interface DataProvider extends AutoCloseable
{
    /**
     * @return True if the data may no longer be accessed.
     */
    boolean isClosed();

    @Override
    void close();

    /**
     * Tells the data provider to move to the next row
     * @return Whether or not there is another row to move to
     * @throws IllegalStateException Thrown if the data has been closed down
     * @throws IndexOutOfBoundsException Thrown if the provider attempts to
     * move beyond its own boundaries
     */
    boolean next();

    /**
     * Tells the data provider to move to the previous row
     * @return Whether or not there was a row to move to
     * @throws IllegalStateException Thrown if the data has been closed down
     * @throws IndexOutOfBoundsException Thrown if the provider attempts to
     * move beyond its own boundaries
     */
    boolean back();

    /**
     * Moves to the last row in the data
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    void toEnd();

    /**
     * Moves to the first row in the data
     * @throws IOException Thrown if an I/O error occurred while attempting to
     * reset the source for that data being provided
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    void reset() throws IOException;

    /**
     * @param columnName The name of the column to look for
     * @return The numerical index of the column stored internally
     * @throws IllegalStateException Thrown if the data has been closed down
     * @throws IndexOutOfBoundsException Thrown if the column does not exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    int getColumnIndex(final String columnName);

    /**
     * @return A list of column names
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    List<String> getColumnNames();

    /**
     * @return The row that the data provider is currently on
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    int getRowIndex();

    /**
     * @param columnName The name of the column to look in
     * @return Whether or not the value in the current row for the specified column is null
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    boolean isNull(final String columnName);

    /**
     * @param columnName The name of the column to look for
     * @return Whether or not the desired column is present
     */
    boolean hasColumn(final String columnName);

    /**
     * @return Whether or not the data provider contains data
     */
    boolean isEmpty();

    /**
     * @return Every value held within the current row
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    Object[] getRowValues();

    /**
     * @param columnName The name of the column containing the desired value
     * @return The object stored within the desired column in the current row
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Object getObject(final String columnName);

    /**Retrieves the value of the designated column in the current row as a boolean
     * <br>
     * If the designated column has a datatype of <code>String</code> and contains a
     * '0' or has a datatype of <code>byte</code>, <code>short</code>, <code>int</code>,
     * or <code>long</code> and is equal to 0, <code>false</code> is returned.
     * <code>false</code> is returned if the value was <code>null</code>
     * @param columnName The name of the column containing the desired boolean value
     * @return The boolean representation of the contained value
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    boolean getBoolean(final String columnName);

    /**
     * @param columnName The name of the column containing the desired value
     * @return The value contained within the column as a string
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    String getString(final String columnName);

    /**
     * @param columnName The name of the column containing the desired value
     * @return The value contained within the column as a URI
     * @throws IllegalStateException Thrown if the data has been closed down or
     * the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    URI getURI(final String columnName);
    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>byte</code> value contained within the desired column.
     * If the value is <code>null</code>, the returned value is <code>null</code>
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Byte getByte(final String columnName);

    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>short</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>null</code>
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Short getShort(final String columnName);

    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>int</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>null</code>.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Integer getInt(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>long</code>
     * @return The <code>long</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>null</code>.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Long getLong(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>float</code>
     * @return The <code>float</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>null</code>.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Float getFloat(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>double</code>
     * @return The <code>double</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is 
     * {@link MissingValues#DOUBLE}. See #56214-240 and later.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    double getDouble(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>BigDecimal</code>
     * @return The <code>BigDecimal</code> value contained within the
     * desired column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    BigDecimal getBigDecimal(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>double</code> array
     * @return The <code>double</code> value contained within the desired
     * column.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Double[] getDoubleArray(final String columnName);

    /**
     * @param columnName The name of the column containing the desired <code>int</code> array
     * @return The <code>int</code> values contained within the desired column
     * @throws IllegalStateException Thrown if the data has been closed down or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Integer[] getIntegerArray(final String columnName);

    /**
     * @param columnName The name of the column containing the desired <code>String</code> array
     * @return The <code>String</code> values contained within the desired column
     * @throws IllegalStateException Thrown if the data has been closed down or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    String[] getStringArray(final String columnName);    
    
    /**
     * @param columnName The name of the column containing a time
     *                   representation
     * @return A time object taken from the object in the given column.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    LocalTime getTime(final String columnName);

    /**
     * @param columnName The name of the column containing a date
     *                   representation
     * @return A date object taken from the object in the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    LocalDate getDate(final String columnName);

    /**
     * @param columnName The name of the column containing a date and time
     *                   representation
     * @return A date and time object at UTC taken from the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    OffsetDateTime getOffsetDateTime(final String columnName);

    /**
     * @param columnName The name of the column containing a date and time
     *                   representation
     * @return A date and time object at UTC take from the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    LocalDateTime getLocalDateTime( final String columnName);

    /**
     * @param columnName The name of the column containing some representation
     *                   that may be converted into an instant
     * @return An instant object converted from the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Instant getInstant(final String columnName);

    /**
     * @param columnName The name of the column containing some representation
     *                   that may be converted into a duration
     * @return A duration object converted from the given column
     * @throws IllegalStateException Thrown if the data has been closed down or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Duration getDuration( final String columnName);

    /**
     * @param columnName The name of the column with the desired value
     * @param <V> The type of the value that will be returned
     * @return The value from the given column on the current row
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    <V> V getValue(final String columnName);

    /**
     * Get the string representation of the value in the column that may be used to form scripts
     * @param columnName The name of the column containing the value of interest
     * @return The string representation of the value
     */
    default String toString(final String columnName)
    {
        Object value = this.getObject( columnName );

        if (value == null)
        {
            return null;
        }

        return value.toString();
    }

    /**
     * Copies the data within the provider from the current row through the last row into the the schema and table
     * <br>
     * The position of the provider will be at the end of the dataset after function completion
     * @param database The database to use.
     * TODO: implementations may not have/use a database, may want to not
     *       require the database as a param but as a member of implementations.
     * @param table Fully qualified table name to copy data into
     * @throws wres.io.ingesting.IngestException When the copy fails.
     */
    default void copy( Database database, final String table )
    {
        this.copy( database, table, false );
    }

    /**
     * Copies the data within the provider from the current row through the last row into the the schema and table
     * <br>
     * The position of the provider will be at the end of the dataset after function completion
     * @param database The database to use
     * TODO: implementations may not have/use a database, may want to not
     *       require the database as a param but as a member of implementations.
     * @param table Fully qualified table name to copy data into
     * @param showProgress Whether or not to show progress during the copy operation
     * @throws wres.io.ingesting.IngestException When the copy fails.
     */
    default void copy( Database database, final String table, final boolean showProgress )
    {
        List<String> columnNames = this.getColumnNames();
        List<String[]> values = new ArrayList<>();
        boolean[] charColumns = new boolean[columnNames.size()];

        for ( int i = 0; i < charColumns.length; i++ )
        {
            // TODO: Find the type of the data accurately instead of all false.
            charColumns[i] = false;
        }

        while (this.next())
        {
            String[] row = new String[columnNames.size()];

            for ( int col = 0; col < columnNames.size(); col++ )
            {
                String representation = this.toString( columnNames.get( col ) );
                row[col] = representation;
            }

            values.add( row );
        }

        // Until we can figure out how to get exceptions to propagate from
        // submitting to the Database executor, run synchronously in caller's
        // Thread.
        database.copy( table,
                       columnNames,
                       values,
                       charColumns );
    }

    /**
     * Converts a CSV file to a DataProvider with the top line being the header
     * @param fileName The path to the csv file
     * @param delimiter The delimiter separating values
     * @return A DataProvider containing the provided CSV data
     * @throws IOException Thrown if the file could not be read
     */
    static DataProvider fromCSV( final URI fileName, final String delimiter) throws IOException
    {
        return CSVDataProvider.from(fileName, delimiter);
    }

    /**
     * Converts a CSV stream to a DataProvider with the top line being the header
     * @param inputStream The CSV stream
     * @param delimiter The delimiter separating values
     * @return A DataProvider containing the provided CSV data
     * @throws IOException Thrown if the file could not be read
     */
    static DataProvider fromCSV( final InputStream inputStream, final String delimiter) throws IOException
    {
        return CSVDataProvider.from( inputStream, delimiter );
    }
    
    /**
     * Converts a CSV file to a DataProvider with the provided column names
     * @param fileName The path to the csv file
     * @param delimiter The delimiter separating values
     * @param columnNames The names of each column
     * @return A DataProvider containing the provided CSV data
     * @throws IOException Thrown if the file could not be read
     */
    static DataProvider fromCSV(
            final URI fileName,
            final String delimiter,
            final String... columnNames)
            throws IOException
    {
        Map<String, Integer> columnIndices = new HashMap<>(  );
        for (int i = 0; i < columnNames.length; ++i)
        {
            columnIndices.put(columnNames[i], i);
        }
        return CSVDataProvider.from( fileName, delimiter, columnIndices );
    }

    /**
     * Creates a JSON String representation of the DataProvider
     * <br>
     * <b>Warning:</b> The DataProvider will move to the end of the
     * data as a result of JSON creation. Buffered implementations
     * will not be able to return to the beginning of their data
     * @return The DataProvider represented as a JSON String
     */
    default String toJSONString()
    {
        return this.toJSON().toString();
    }

    /**
     * Creates a JSON representation of the DataProvider
     * <br>
     * <b>Warning:</b> The DataProvider will move to the end of the
     * data as a result of JSON creation. Buffered implementations
     * will not be able to return to the beginning of their data
     * @return The DataProvider represented as JSON
     */
    default JsonValue toJSON()
    {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

        do
        {
            JsonObjectBuilder row = Json.createObjectBuilder();

            for (String column : this.getColumnNames())
            {
                row.add( column, this.getString( column ) );
            }

            arrayBuilder.add( row );

        } while (this.next());

        return arrayBuilder.build();
    }
}

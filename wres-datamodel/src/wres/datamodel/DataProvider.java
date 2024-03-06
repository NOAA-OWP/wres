package wres.datamodel;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.sql.ResultSet;

/**
 * <p>Provides access to, and operations upon, tabular data.
 *
 * <p>Column names used as input to getter methods are case-insensitive, much like a {@link ResultSet}.
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
     * @return Whether there is another row to move to
     * @throws IllegalStateException Thrown if the data has been closed down
     * @throws IndexOutOfBoundsException Thrown if the provider attempts to
     * move beyond its own boundaries
     */
    boolean next();

    /**
     * Tells the data provider to move to the previous row
     * @return Whether there was a row to move to
     * @throws IllegalStateException Thrown if the data has been closed down
     * @throws IndexOutOfBoundsException Thrown if the provider attempts to
     * move beyond its own boundaries
     */
    boolean back();

    /**
     * @param columnName The name of the column to look for
     * @return The numerical index of the column stored internally
     * @throws IllegalStateException Thrown if the data has been closed down
     * @throws IndexOutOfBoundsException Thrown if the column does not exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    int getColumnIndex( final String columnName );

    /**
     * @return A list of column names
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    List<String> getColumnNames();

    /**
     * @param columnName The name of the column to look in
     * @return Whether the value in the current row for the specified column is null
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    boolean isNull( final String columnName );

    /**
     * @param columnName The name of the column to look for
     * @return Whether the desired column is present
     */
    boolean hasColumn( final String columnName );

    /**
     * @return Whether the data provider contains data
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
    Object getObject( final String columnName );

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
    boolean getBoolean( final String columnName );

    /**
     * @param columnName The name of the column containing the desired value
     * @return The value contained within the column as a string
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    String getString( final String columnName );

    /**
     * @param columnName The name of the column containing the desired value
     * @return The value contained within the column as a URI
     * @throws IllegalStateException Thrown if the data has been closed down or
     * the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    URI getURI( final String columnName );

    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>byte</code> value contained within the desired column.
     * If the value is <code>null</code>, the returned value is <code>null</code>
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Byte getByte( final String columnName );

    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>short</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>null</code>
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Short getShort( final String columnName );

    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>int</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>null</code>.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Integer getInt( final String columnName );

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
    Long getLong( final String columnName );

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
    Float getFloat( final String columnName );

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
    double getDouble( final String columnName );

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>BigDecimal</code>
     * @return The <code>BigDecimal</code> value contained within the
     * desired column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    BigDecimal getBigDecimal( final String columnName );

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>double</code> array
     * @return The <code>double</code> value contained within the desired
     * column.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Double[] getDoubleArray( final String columnName );

    /**
     * @param columnName The name of the column containing the desired <code>int</code> array
     * @return The <code>int</code> values contained within the desired column
     * @throws IllegalStateException Thrown if the data has been closed down or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Integer[] getIntegerArray( final String columnName );

    /**
     * @param columnName The name of the column containing a time
     *                   representation
     * @return A time object taken from the object in the given column.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    LocalTime getTime( final String columnName );

    /**
     * @param columnName The name of the column containing a date
     *                   representation
     * @return A date object taken from the object in the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    LocalDate getDate( final String columnName );

    /**
     * @param columnName The name of the column containing a date and time
     *                   representation
     * @return A date and time object at UTC taken from the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    OffsetDateTime getOffsetDateTime( final String columnName );

    /**
     * @param columnName The name of the column containing a date and time
     *                   representation
     * @return A date and time object at UTC take from the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    LocalDateTime getLocalDateTime( final String columnName );

    /**
     * @param columnName The name of the column containing some representation
     *                   that may be converted into an instant
     * @return An instant object converted from the given column
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Instant getInstant( final String columnName );

    /**
     * @param columnName The name of the column containing some representation
     *                   that may be converted into a duration
     * @return A duration object converted from the given column
     * @throws IllegalStateException Thrown if the data has been closed down or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    Duration getDuration( final String columnName );

    /**
     * @param columnName The name of the column with the desired value
     * @param <V> The type of the value that will be returned
     * @return The value from the given column on the current row
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    <V> V getValue( final String columnName );

    /**
     * Get the string representation of the value in the column that may be used to form scripts
     * @param columnName The name of the column containing the value of interest
     * @return The string representation of the value
     */
    default String toString( final String columnName )
    {
        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }

        return value.toString();
    }
}

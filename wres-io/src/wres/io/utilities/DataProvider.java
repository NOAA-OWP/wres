package wres.io.utilities;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import wres.util.functional.ExceptionalConsumer;
import wres.util.functional.ExceptionalFunction;

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
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    void reset();

    /**
     * @param columnName The name of the column to look for
     * @return The numerical index of the column stored internally
     * @throws IllegalStateException Thrown if the data has been closed down
     * @throws IndexOutOfBoundsException Thrown if the column does not exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    int getColumnIndex(final String columnName);

    /**
     * @return A series of column names to iterate through
     * @throws IllegalStateException Thrown if the data has been closed down
     */
    Iterable<String> getColumnNames();

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
     * Executes a method on every row in the data
     * @param consumer A method to call on every row
     * @param <E> A type of error that may be thrown
     * @throws E Thrown if something in the method fails
     */
    default <E extends Exception> void consume( ExceptionalConsumer<DataProvider, E> consumer) throws E
    {
        while (this.next())
        {
            consumer.accept( this );
        }
    }

    /**
     * Calls a function on every row of the data and returns a list of the results
     * @param interpretor The function to call on every row
     * @param <U> The type of value that is returned from the function
     * @param <E> The type of error that the function may throw
     * @return A collection of the results from the passed in function
     * @throws E Thrown if something in the passed in function fails
     */
    default <U, E extends Exception> Collection<U> interpret(
            ExceptionalFunction<DataProvider, U, E> interpretor
    ) throws E
    {
        List<U> result = new ArrayList<>();

        while (this.next())
        {
            result.add(interpretor.call( this ));
        }

        return result;
    }

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
     * @return The <code>byte</code> value contained within the desired column.
     * If the value is <code>null</code>, the returned value is <code>0</code>
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    byte getByte(final String columnName);

    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>short</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>0</code>
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    short getShort(final String columnName);

    /**
     * @param columnName The name of the column containing the desired value
     * @return The <code>int</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>0</code>.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    int getInt(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>long</code>
     * @return The <code>long</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>0</code>.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    long getLong(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>float</code>
     * @return The <code>float</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is
     * <code>0</code>.
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    float getFloat(final String columnName);

    /**
     * @param columnName The name of the column containing the desired
     *                   <code>double</code>
     * @return The <code>double</code> value contained within the desired
     * column. If the value is <code>null</code>, the returned value is 0
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
     * @param columnName The name of the column with the desired value
     * @param <V> The type of the value that will be returned
     * @return The value from the given column on the current row
     * @throws IllegalStateException Thrown if the data has been closed down
     * or the column name doesn't exist
     * @throws IndexOutOfBoundsException Thrown if the data is empty
     */
    <V> V getValue(final String columnName);
}

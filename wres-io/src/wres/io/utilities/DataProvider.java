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

    boolean next();
    boolean back();
    void toEnd();
    void reset();
    
    int getColumnIndex(final String columnName);
    Iterable<String> getColumnNames();

    int getRowIndex();

    boolean isNull(final String columnName);
    boolean hasColumn(final String columnName);
    boolean isEmpty();

    default <E extends Exception> void consume( ExceptionalConsumer<DataProvider, E> consumer) throws E
    {
        while (this.next())
        {
            consumer.accept( this );
        }
    }

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

    Object[] getRowValues();

    Object getObject(final String columnName);

    boolean getBoolean(final String columnName);

    String getString(final String columnName);

    int getInt(final String columnName);

    short getShort(final String columnName);

    long getLong(final String columnName);

    float getFloat(final String columnName);

    double getDouble(final String columnName);

    double[] getDoubleArray(final String columnName);

    BigDecimal getBigDecimal(final String columnName);

    LocalTime getTime(final String columnName);

    LocalDate getDate(final String columnName);

    OffsetDateTime getOffsetDateTime(final String columnName);

    LocalDateTime getLocalDateTime( final String columnName);

    Instant getInstant(final String columnName);

    <V> V getValue(final String columnName);
}

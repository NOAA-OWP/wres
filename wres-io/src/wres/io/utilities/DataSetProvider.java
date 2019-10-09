package wres.io.utilities;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import wres.datamodel.MissingValues;
import wres.util.TimeHelper;

/**
 * A fully in-memory tabular dataset that doesn't require an
 * active connection to a database. Mimics the behavior of
 * the <code>ResultSet</code> data structure used for
 * sql queries.
 */
public class DataSetProvider implements DataProvider
{
    private Map<String, Integer> columnNames;
    private List<Object[]> rows;
    private int currentRow = -1;
    private boolean closed;

    private DataSetProvider(){
        columnNames = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        rows = new ArrayList<>(  );
    }

    public static DataSetProvider from(final DataProvider provider)
    {
        if (provider == null || provider.isClosed())
        {
            throw new IllegalArgumentException(
                    "An open DataProvider must be supplied to create a DataSetProvider."
            );
        }

        DataSetProvider dataSetProvider = new DataSetProvider(  );

        Iterable<String> columns = provider.getColumnNames();

        int columnIndex = 0;

        for (String columnName : columns)
        {
            dataSetProvider.columnNames.put( columnName, columnIndex );
            columnIndex++;
        }

        while (provider.next())
        {
            dataSetProvider.rows.add( provider.getRowValues() );
        }

        return dataSetProvider;
    }

    static DataSetProvider from(final Map<String, Integer> columnNames, final List<Object[]> rows )
    {
        DataSetProvider provider = new DataSetProvider();
        provider.columnNames.putAll( columnNames );
        provider.rows.addAll(rows);
        return provider;
    }

    @Override
    public boolean isClosed()
    {
        return this.closed;
    }

    @Override
    public boolean next()
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The position in the dataset "
                                             + "may not be incremented." );
        }
        else if (this.currentRow == rows.size())
        {
            throw new IndexOutOfBoundsException( "The position in the dataset may"
                                                 + "not move beyond the size of its data." );
        }
        this.currentRow++;
        return this.currentRow < rows.size();
    }

    @Override
    public boolean back()
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The position in the data set "
                                             + "may not be decremented.");
        }
        else if (this.currentRow == 0)
        {
            throw new IndexOutOfBoundsException( "Position in the dataset may"
                                                 + "not move before its beginning." );
        }

        this.currentRow--;
        return this.currentRow >= 0;
    }

    @Override
    public void toEnd()
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The position in the data set may "
                                             + "not be moved to the end.");
        }

        this.currentRow = this.rows.size() - 1;
    }

    @Override
    public void reset()
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The position in the data set may not "
                                             + "be moved back to its beginning.");
        }

        if (this.rows.isEmpty())
        {
            this.currentRow = -1;
        }
        else
        {
            this.currentRow = 0;
        }
    }

    @Override
    public boolean isNull( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        return this.getObject( currentRow, this.getColumnIndex( columnName ) ) == null;
    }

    @Override
    public boolean hasColumn( String columnName )
    {
        return this.columnNames.containsKey( columnName );
    }

    @Override
    public boolean isEmpty()
    {
        return this.rows.isEmpty();
    }

    @Override
    public Object[] getRowValues()
    {
        return this.rows.get( this.currentRow );
    }

    /**
     * Retrieves the value from the indicated row stored in the indicated column
     * @param rowNumber The row to look int
     * @param index The index for the column containing the value
     * @return The value stored in the row and column
     */
    private Object getObject(int rowNumber, int index)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        // If "next" hasn't been called, go ahead and move on to the next row
        if (rowNumber < 0)
        {
            rowNumber++;
        }

        if (this.rows == null)
        {
            throw new IndexOutOfBoundsException( "There is no data contained within the data set." );
        }
        else if (rowNumber >= this.rows.size())
        {
            throw new IndexOutOfBoundsException( "There are no more rows to retrieve data from. Index = " +
                                                 String.valueOf(rowNumber) +
                                                 ", Row Count: " +
                                                 String.valueOf(this.rows.size()) );
        }
        else if (this.rows.get( rowNumber ).length <= index)
        {
            throw new IndexOutOfBoundsException( "The provided index exceeds the length of the row. Index = " +
                                                 String.valueOf(index) +
                                                 " Row Length: " +
                                                 String.valueOf(this.rows.get(rowNumber).length) );
        }

        return this.rows.get(rowNumber)[index];
    }

    @Override
    public int getColumnIndex(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.columnNames == null)
        {
            throw new IndexOutOfBoundsException( "There is no data to retrieve from this data set" );
        }

        Integer index = this.columnNames.getOrDefault( columnName, -1 );

        if (index < 0)
        {
            throw new IndexOutOfBoundsException( "There is no column in the data set named " + columnName );
        }

        return index;
    }

    @Override
    public Iterable<String> getColumnNames()
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The dataset is not accessible.");
        }
        return columnNames.keySet();
    }

    @Override
    public int getRowIndex()
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        return this.currentRow;
    }

    @Override
    public Object getObject(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.currentRow < 0)
        {
            this.currentRow++;
        }

        if (this.isNull( columnName ))
        {
            return null;
        }

        return this.getObject(this.currentRow, this.getColumnIndex( columnName ));
    }

    @Override
    public boolean getBoolean( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject( columnName );

        if (value == null)
        {
            return false;
        }
        else if (value instanceof Boolean)
        {
            return (Boolean)value;
        }
        else if (value instanceof Integer)
        {
            return (Integer)value == 1;
        }
        else if (value instanceof Short)
        {
            return (Short)value == 1;
        }
        else if (value instanceof Long)
        {
            return (Long)value == 1;
        }

        return value.toString().contains( "1" );
    }

    @Override
    public Byte getByte( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);

        if (value == null)
        {
            // Return the default byte
            return null;
        }
        else if (value instanceof Byte)
        {
            return (byte)value;
        }
        else if (value instanceof Number)
        {
            // Use Number to convert a numerical type to a byte
            return ((Number)value).byteValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a byte, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Byte.parseByte( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString() +
                    "' with the value '" + value.toString() +
                    "' in the column '" + columnName +
                    "' cannot be casted as a byte." );
        }
    }

    @Override
    public Integer getInt(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);

        if (value == null)
        {
            return null;
        }
        else if (value instanceof Integer)
        {
            return (int)value;
        }
        else if (value instanceof Number)
        {
            return ((Number)value).intValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a int, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Integer.parseInt( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString() +
                    "' with the value '" + value.toString() +
                    "' in the column '" + columnName +
                    "' cannot be casted as an integer." );
        }
    }

    @Override
    public Short getShort(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);

        if (value == null)
        {
            return null;
        }
        else if (value instanceof Short)
        {
            return (short)value;
        }
        else if (value instanceof Number)
        {
            return ((Number)value).shortValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a short, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Short.parseShort( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString() +
                    "' with the value '" + value.toString() +
                    "' in the column '" + columnName +
                    "' cannot be casted as a short." );
        }
    }

    @Override
    public Long getLong(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject( columnName );

        if (value == null)
        {
            return null;
        }
        else if (value instanceof Long)
        {
            return (long)value;
        }
        else if (value instanceof Number)
        {
            return ((Number)value).longValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a long, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Long.parseLong( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString() +
                    "' with the value '" + value.toString() +
                    "' in the column '" + columnName +
                    "' cannot be casted as a long." );
        }
    }

    @Override
    public Float getFloat(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);

        if (value == null)
        {
            return null;
        }
        else if (value instanceof Float)
        {
            return (float)value;
        }
        else if (value instanceof Number)
        {
            return ( ( Number ) value ).floatValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a float, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Float.parseFloat( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString() +
                    "' with the value '" + value.toString() +
                    "' in the column '" + columnName +
                    "' cannot be casted as a float." );
        }
    }

    @Override
    public double getDouble(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);

        if (value == null)
        {
            return MissingValues.DOUBLE;
        }
        else if (value instanceof Double)
        {
            return (double)this.getObject( columnName );
        }
        else if (value instanceof Number)
        {
            return ((Number)value).doubleValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a double, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Double.parseDouble( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString() +
                    "' with the value '" + value.toString() +
                    "' in the column '" + columnName +
                    "' cannot be casted as a double." );
        }
    }

    @Override
    public Double[] getDoubleArray( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object array = this.getObject(columnName);

        if (array == null)
        {
            return null;
        }
        else if (array instanceof Double[])
        {
            return (Double[])this.getObject(columnName);
        }
        else if (array instanceof Number[])
        {
            Number[] numbers = (Number[])array;
            Double[] result = new Double[numbers.length];

            for (int i = 0; i < numbers.length; ++i)
            {
                result[i] = numbers[i].doubleValue();
            }

            return result;
        }
        else if (array instanceof String[])
        {
            String[] numbers = (String[])array;
            Double[] result = new Double[numbers.length];

            for (int i = 0; i < numbers.length; ++i)
            {
                result[i] = Double.parseDouble( numbers[i] );
            }

            return result;
        }

        throw new ClassCastException( "The type '" +
                                      array.getClass().toString() +
                                      "' in the column '" + columnName +
                                      "' cannot be casted as a double array." );
    }

    @Override
    public Integer[] getIntegerArray( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object array = this.getObject(columnName);

        if (array == null)
        {
            return null;
        }
        else if (array instanceof Integer[])
        {
            return (Integer[])this.getObject(columnName);
        }
        else if (array instanceof Number[])
        {
            Number[] numbers = (Number[])array;
            Integer[] result = new Integer[numbers.length];

            for (int i = 0; i < numbers.length; ++i)
            {
                result[i] = numbers[i].intValue();
            }

            return result;
        }
        else if (array instanceof String[])
        {
            String[] numbers = (String[])array;
            Integer[] result = new Integer[numbers.length];

            for (int i = 0; i < numbers.length; ++i)
            {
                result[i] = Integer.parseInt( numbers[i] );
            }

            return result;
        }

        throw new ClassCastException( "The type '" +
                                      array.getClass().toString() +
                                      "' in the column '" + columnName +
                                      "' cannot be casted as a integer array." );
    }

    @Override
    public BigDecimal getBigDecimal( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);

        if (value == null)
        {
            return null;
        }
        else if (value instanceof Double)
        {
            return new BigDecimal( (Double)value );
        }
        else if (value instanceof Float)
        {
            return new BigDecimal( (Float)value );
        }
        else if (value instanceof Integer)
        {
            return new BigDecimal( (Integer)value );
        }
        else if (value instanceof Long)
        {
            return new BigDecimal( (Long)value );
        }
        else if (value instanceof Short)
        {
            return new BigDecimal( (Short)value );
        }
        else if (value instanceof Byte)
        {
            return new BigDecimal( (Byte)value );
        }

        try
        {
            // If the string representation of whatever object can be used as a BigDecimal, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return new BigDecimal( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString() +
                    "' with the value '" + value.toString() +
                    "' in the column '" + columnName +
                    "' cannot be casted as a BigDecimal." );
        }
    }

    @Override
    public LocalTime getTime( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject( columnName );

        if (value instanceof LocalTime)
        {
            return (LocalTime)this.getObject( columnName );
        }
        else if (value instanceof LocalDateTime)
        {
            return ((LocalDateTime)value).toLocalTime();
        }
        else if (value instanceof OffsetDateTime)
        {
            return ((OffsetDateTime)value).toLocalTime();
        }

        Instant instant = this.getInstant( columnName );
        return instant.atOffset( ZoneOffset.UTC ).toLocalTime();
    }

    @Override
    public LocalDate getDate( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject( columnName );

        if (value instanceof LocalDate)
        {
            return (LocalDate)this.getObject( columnName );
        }
        else if (value instanceof LocalDateTime)
        {
            return ((LocalDateTime)value).toLocalDate();
        }
        else if (value instanceof OffsetDateTime)
        {
            return ((OffsetDateTime)value).toLocalDate();
        }

        Instant instant = this.getInstant( columnName );
        return LocalDateTime.ofInstant( instant, ZoneId.of( "UTC" )).toLocalDate();
    }

    @Override
    public OffsetDateTime getOffsetDateTime( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject( columnName );

        if (value instanceof OffsetDateTime)
        {
            return (OffsetDateTime)value;
        }

        // Since we know this isn't natively an offset date time, we try to
        // convert it to a type that CAN be an offset date time.
        Instant instant = this.getInstant( columnName );
        return OffsetDateTime.ofInstant( instant, ZoneId.of( "UTC" ) );
    }

    @Override
    public LocalDateTime getLocalDateTime( String columnName )
    {
        return this.getOffsetDateTime( columnName ).toLocalDateTime();
    }

    @Override
    public Instant getInstant( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject( columnName  );

        if (value == null)
        {
            return null;
        }

        Instant result;

        if (value instanceof Instant)
        {
            result = (Instant)value;
        }
        else if (value instanceof OffsetDateTime)
        {
            result = ((OffsetDateTime)value).toInstant();
        }
        else if (value instanceof LocalDateTime)
        {
            result = ((LocalDateTime)value).toInstant( ZoneOffset.UTC );
        }
        // Timestamps are interpretted as strings in order to avoid the 'help'
        // that JDBC provides by converting timestamps to local times and
        // applying daylight savings changes
        else if ( value instanceof String )
        {
            String stringRepresentation = value.toString();
            stringRepresentation = stringRepresentation.replace( " ", "T" );

            if (!stringRepresentation.endsWith( "Z" ))
            {
                stringRepresentation += "Z";
            }

            result = Instant.parse( stringRepresentation );
        }
        else if (value instanceof Integer)
        {
            result = Instant.ofEpochSecond( (Integer)value );
        }
        else if (value instanceof Long)
        {
            result = Instant.ofEpochSecond( (Long)value );
        }
        else if (value instanceof Double)
        {
            Double epochSeconds = (Double)value;
            result = Instant.ofEpochSecond( epochSeconds.longValue() );
        }
        else
        {
            throw new IllegalArgumentException( "The type for the column named '" +
                                                columnName +
                                                " cannot be converted into an Instant.");
        }

        return result;
    }

    public Duration getDuration(String columnName)
    {
        Duration result;

        Object value = this.getObject( columnName );

        if (value == null)
        {
            return null;
        }
        else if (value instanceof Duration)
        {
            result = (Duration)value;
        }
        else if (value instanceof Number)
        {
            result = Duration.of( this.getLong( columnName ), TimeHelper.LEAD_RESOLUTION );
        }
        else if (value instanceof String)
        {
            result = Duration.parse( value.toString() );
        }
        else
        {
            throw new IllegalArgumentException( "The type for the column named '" +
                                                columnName +
                                                "' cannot be converted into a Duration." );
        }

        return result;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <V> V getValue( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }
        return (V)this.getObject( columnName );
    }

    @Override
    public String getString(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);

        if (value == null)
        {
            return null;
        }

        return String.valueOf(value);
    }

    @Override
    public URI getURI( String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject(columnName);
        URI uri;

        if (value == null)
        {
            return null;
        }
        else if (value instanceof URI)
        {
            uri = (URI)value;
        }
        else if (value instanceof URL )
        {
            try
            {
                uri = ((URL)value).toURI();
            }
            catch ( URISyntaxException e )
            {
                throw new IllegalArgumentException( "The value '" + value + "' cannot be converted into a URI", e );
            }
        }
        else
        {
            uri = URI.create( String.valueOf( value ) );
        }

        return uri;
    }

    @Override
    public void close()
    {
        this.closed = true;
        this.columnNames = null;
        this.rows = null;
        this.currentRow = -1;
    }
}

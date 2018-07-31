package wres.io.utilities;

import java.math.BigDecimal;
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
        columnNames = new TreeMap<>(  );
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
    public byte getByte( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.isNull( columnName ))
        {
            return 0;
        }

        return (byte)this.getObject( columnName );
    }

    @Override
    public int getInt(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.isNull( columnName ))
        {
            return 0;
        }

        return (int)this.getObject( columnName );
    }

    @Override
    public short getShort(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.isNull( columnName ))
        {
            return 0;
        }

        return (short)this.getObject( columnName );
    }

    @Override
    public long getLong(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.isNull( columnName ))
        {
            return 0;
        }

        return (long)this.getObject( columnName );
    }

    @Override
    public float getFloat(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.isNull( columnName ))
        {
            return 0;
        }

        return (float)this.getObject( columnName );
    }

    @Override
    public double getDouble(String columnName)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        if (this.isNull( columnName ))
        {
            return 0;
        }

        return (double)this.getObject( columnName );
    }

    @Override
    public Double[] getDoubleArray( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }
        return (Double[])this.getObject(columnName);
    }

    @Override
    public BigDecimal getBigDecimal( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }
        return (BigDecimal)this.getObject( columnName );
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
        return String.valueOf(this.getObject( columnName ));
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

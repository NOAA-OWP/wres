package wres.datamodel.types;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.ResultSet;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.DataProvider;
import wres.datamodel.MissingValues;
import wres.datamodel.time.TimeSeriesSlicer;

/**
 * A fully in-memory tabular dataset that does not require an active connection to a database. Mimics the behavior of
 * the {@link ResultSet} data structure used for sql queries.
 */
public class TabularDataset implements DataProvider
{
    private static final String THE_DATA_SET_IS_INACCESSIBLE = "The data set is inaccessible.";
    private static final Logger LOGGER = LoggerFactory.getLogger( TabularDataset.class );
    private static final String THE_TYPE = "The type '";
    private static final String WITH_THE_VALUE = "' with the value '";
    private static final String IN_THE_COLUMN = "' in the column '";
    private Map<String, Integer> columnNames;
    private List<Object[]> rows;
    private int currentRow = -1;
    private boolean closed;

    private TabularDataset()
    {
        columnNames = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        rows = new ArrayList<>();
    }

    /**
     * Creates an instance from a provider.
     * @param provider the provider
     * @return an instance
     */
    public static TabularDataset from( final DataProvider provider )
    {
        if ( provider == null || provider.isClosed() )
        {
            throw new IllegalArgumentException(
                    "An open DataProvider must be supplied to create a TabularDataset."
            );
        }

        TabularDataset dataSetProvider = new TabularDataset();

        List<String> columns = provider.getColumnNames();
        LOGGER.debug( "Created TabularDataset (1) with columns {}", columns );

        int columnIndex = 0;

        for ( String columnName : columns )
        {
            dataSetProvider.columnNames.put( columnName, columnIndex );
            columnIndex++;
        }

        LOGGER.debug( "TabularDataset (1) now has columnNames {}",
                      dataSetProvider.columnNames );

        while ( provider.next() )
        {
            dataSetProvider.rows.add( provider.getRowValues() );
        }

        return dataSetProvider;
    }

    /**
     * Creates an instance from the inputs.
     * @param columnNames the column names
     * @param rows the rows
     * @return the dataset
     */
    public static TabularDataset from( final Map<String, Integer> columnNames, final List<Object[]> rows )
    {
        TabularDataset provider = new TabularDataset();
        LOGGER.debug( "Created TabularDataset (2) with columnNames {}",
                      columnNames );
        provider.columnNames.putAll( columnNames );
        LOGGER.debug( "TabularDataset (2) now has columnNames {}",
                      provider.columnNames );
        provider.rows.addAll( rows );
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
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The position in the dataset "
                                             + "may not be incremented." );
        }
        else if ( this.currentRow == rows.size() )
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
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The position in the data set "
                                             + "may not be decremented." );
        }
        else if ( this.currentRow == 0 )
        {
            throw new IndexOutOfBoundsException( "Position in the dataset may"
                                                 + "not move before its beginning." );
        }

        this.currentRow--;
        return this.currentRow >= 0;
    }

    @Override
    public boolean isNull( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
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
    private Object getObject( int rowNumber, int index )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        // If "next" hasn't been called, go ahead and move on to the next row
        if ( rowNumber < 0 )
        {
            rowNumber++;
        }

        if ( this.rows == null )
        {
            throw new IndexOutOfBoundsException( "There is no data contained within the data set." );
        }
        else if ( rowNumber >= this.rows.size() )
        {
            throw new IndexOutOfBoundsException( "There are no more rows to retrieve data from. Index = " +
                                                 rowNumber +
                                                 ", Row Count: " +
                                                 this.rows.size() );
        }
        else if ( this.rows.get( rowNumber ).length <= index )
        {
            throw new IndexOutOfBoundsException( "The provided index exceeds the length of the row. Index = " +
                                                 index +
                                                 " Row Length: " +
                                                 this.rows.get( rowNumber ).length );
        }

        return this.rows.get( rowNumber )[index];
    }

    @Override
    public int getColumnIndex( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        if ( this.columnNames == null )
        {
            throw new IndexOutOfBoundsException( "There is no data to retrieve from this data set" );
        }

        Integer index = this.columnNames.getOrDefault( columnName, -1 );

        if ( index < 0 )
        {
            throw new IndexOutOfBoundsException( "There is no column in the data set named " + columnName );
        }

        return index;
    }

    @Override
    public List<String> getColumnNames()
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The dataset is not accessible." );
        }
        return new ArrayList<>( columnNames.keySet() );
    }

    @Override
    public Object getObject( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        if ( this.currentRow < 0 )
        {
            this.currentRow++;
        }

        if ( this.isNull( columnName ) )
        {
            return null;
        }

        return this.getObject( this.currentRow, this.getColumnIndex( columnName ) );
    }

    @Override
    public boolean getBoolean( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return false;
        }
        else if ( value instanceof Boolean v )
        {
            return v;
        }
        else if ( value instanceof Integer v )
        {
            return v == 1;
        }
        else if ( value instanceof Short v )
        {
            return v == 1;
        }
        else if ( value instanceof Long v )
        {
            return v == 1;
        }

        return value.toString().contains( "1" );
    }

    @Override
    public Byte getByte( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            // Return the default byte
            return null;
        }
        else if ( value instanceof Byte )
        {
            return ( byte ) value;
        }
        else if ( value instanceof Number v )
        {
            // Use Number to convert a numerical type to a byte
            return v.byteValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a byte, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Byte.parseByte( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_TYPE + value.getClass() +
                                          WITH_THE_VALUE + value +
                                          IN_THE_COLUMN + columnName +
                                          "' cannot be casted as a byte." );
        }
    }

    @Override
    public Integer getInt( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof Integer )
        {
            return ( int ) value;
        }
        else if ( value instanceof Number v )
        {
            return v.intValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a int, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Integer.parseInt( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_TYPE + value.getClass() +
                                          WITH_THE_VALUE + value +
                                          IN_THE_COLUMN + columnName +
                                          "' cannot be casted as an integer." );
        }
    }

    @Override
    public Short getShort( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof Short )
        {
            return ( short ) value;
        }
        else if ( value instanceof Number v )
        {
            return v.shortValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a short, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Short.parseShort( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_TYPE + value.getClass() +
                                          WITH_THE_VALUE + value +
                                          IN_THE_COLUMN + columnName +
                                          "' cannot be casted as a short." );
        }
    }

    @Override
    public Long getLong( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof Long )
        {
            return ( long ) value;
        }
        else if ( value instanceof Number v )
        {
            return v.longValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a long, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Long.parseLong( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_TYPE + value.getClass() +
                                          WITH_THE_VALUE + value +
                                          IN_THE_COLUMN + columnName +
                                          "' cannot be casted as a long." );
        }
    }

    @Override
    public Float getFloat( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof Float )
        {
            return ( float ) value;
        }
        else if ( value instanceof Number v )
        {
            return v.floatValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a float, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Float.parseFloat( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_TYPE + value.getClass() +
                                          WITH_THE_VALUE + value +
                                          IN_THE_COLUMN + columnName +
                                          "' cannot be casted as a float." );
        }
    }

    @Override
    public double getDouble( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return MissingValues.DOUBLE;
        }
        else if ( value instanceof Double )
        {
            return ( double ) this.getObject( columnName );
        }
        else if ( value instanceof Number v )
        {
            return v.doubleValue();
        }

        try
        {
            // If the string representation of whatever object can be used as a double, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Double.parseDouble( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_TYPE + value.getClass() +
                                          WITH_THE_VALUE + value +
                                          IN_THE_COLUMN + columnName +
                                          "' cannot be casted as a double." );
        }
    }

    @Override
    public Double[] getDoubleArray( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object array = this.getObject( columnName );

        if ( array == null )
        {
            return new Double[0];
        }
        else if ( array instanceof Double[] )
        {
            return ( Double[] ) this.getObject( columnName );
        }
        else if ( array instanceof Number[] v )
        {
            Double[] result = new Double[v.length];

            for ( int i = 0; i < v.length; ++i )
            {
                result[i] = v[i].doubleValue();
            }

            return result;
        }
        else if ( array instanceof String[] v )
        {
            Double[] result = new Double[v.length];

            for ( int i = 0; i < v.length; ++i )
            {
                result[i] = Double.parseDouble( v[i] );
            }

            return result;
        }

        throw new ClassCastException( THE_TYPE +
                                      array.getClass() +
                                      IN_THE_COLUMN + columnName +
                                      "' cannot be casted as a double array." );
    }

    @Override
    public Integer[] getIntegerArray( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object array = this.getObject( columnName );

        if ( array == null )
        {
            return new Integer[0];
        }
        else if ( array instanceof Integer[] )
        {
            return ( Integer[] ) this.getObject( columnName );
        }
        else if ( array instanceof Number[] v )
        {
            Integer[] result = new Integer[v.length];

            for ( int i = 0; i < v.length; ++i )
            {
                result[i] = v[i].intValue();
            }

            return result;
        }
        else if ( array instanceof String[] v )
        {
            Integer[] result = new Integer[v.length];

            for ( int i = 0; i < v.length; ++i )
            {
                result[i] = Integer.parseInt( v[i] );
            }

            return result;
        }

        throw new ClassCastException( THE_TYPE +
                                      array.getClass() +
                                      IN_THE_COLUMN + columnName +
                                      "' cannot be casted as a integer array." );
    }

    @Override
    public BigDecimal getBigDecimal( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof Double v )
        {
            return BigDecimal.valueOf( v );
        }
        else if ( value instanceof Float v )
        {
            return BigDecimal.valueOf( v );
        }
        else if ( value instanceof Integer v )
        {
            return new BigDecimal( v );
        }
        else if ( value instanceof Long v )
        {
            return new BigDecimal( v );
        }
        else if ( value instanceof Short v )
        {
            return new BigDecimal( v );
        }
        else if ( value instanceof Byte v )
        {
            return new BigDecimal( v );
        }

        try
        {
            // If the string representation of whatever object can be used as a BigDecimal, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return new BigDecimal( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_TYPE + value.getClass() +
                                          WITH_THE_VALUE + value +
                                          IN_THE_COLUMN + columnName +
                                          "' cannot be casted as a BigDecimal." );
        }
    }

    @Override
    public LocalTime getTime( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value instanceof LocalTime )
        {
            return ( LocalTime ) this.getObject( columnName );
        }
        else if ( value instanceof LocalDateTime v )
        {
            return ( v.toLocalTime() );
        }
        else if ( value instanceof OffsetDateTime v )
        {
            return ( v.toLocalTime() );
        }

        Instant instant = this.getInstant( columnName );
        return instant.atOffset( ZoneOffset.UTC ).toLocalTime();
    }

    @Override
    public LocalDate getDate( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value instanceof LocalDate )
        {
            return ( LocalDate ) this.getObject( columnName );
        }
        else if ( value instanceof LocalDateTime v )
        {
            return v.toLocalDate();
        }
        else if ( value instanceof OffsetDateTime v )
        {
            return v.toLocalDate();
        }

        Instant instant = this.getInstant( columnName );
        return LocalDateTime.ofInstant( instant, ZoneId.of( "UTC" ) ).toLocalDate();
    }

    @Override
    public OffsetDateTime getOffsetDateTime( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value instanceof OffsetDateTime v )
        {
            return v;
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
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }

        Instant result;

        if ( value instanceof Instant v )
        {
            result = v;
        }
        else if ( value instanceof OffsetDateTime v )
        {
            result = ( v.toInstant() );
        }
        else if ( value instanceof LocalDateTime v )
        {
            result = ( v.toInstant( ZoneOffset.UTC ) );
        }
        // Timestamps are interpretted as strings in order to avoid the 'help'
        // that JDBC provides by converting timestamps to local times and
        // applying daylight savings changes
        else if ( value instanceof String )
        {
            String stringRepresentation = value.toString();
            stringRepresentation = stringRepresentation.replace( " ", "T" );

            if ( !stringRepresentation.endsWith( "Z" ) )
            {
                stringRepresentation += "Z";
            }

            result = Instant.parse( stringRepresentation );
        }
        else if ( value instanceof Integer v )
        {
            result = Instant.ofEpochSecond( v );
        }
        else if ( value instanceof Long v )
        {
            result = Instant.ofEpochSecond( v );
        }
        else if ( value instanceof Double epochSeconds )
        {
            result = Instant.ofEpochSecond( epochSeconds.longValue() );
        }
        else
        {
            throw new IllegalArgumentException( "The type for the column named '" +
                                                columnName +
                                                " cannot be converted into an Instant." );
        }

        return result;
    }

    public Duration getDuration( String columnName )
    {
        Duration result;

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof Duration v )
        {
            result = v;
        }
        else if ( value instanceof Number )
        {
            result = Duration.of( this.getLong( columnName ), TimeSeriesSlicer.LEAD_RESOLUTION );
        }
        else if ( value instanceof String )
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
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }
        return ( V ) this.getObject( columnName );
    }

    @Override
    public String getString( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
        }

        return String.valueOf( value );
    }

    @Override
    public URI getURI( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );
        URI uri;

        if ( value == null )
        {
            return null;
        }
        else if ( value instanceof URI v )
        {
            uri = v;
        }
        else if ( value instanceof URL v )
        {
            try
            {
                uri = v.toURI();
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

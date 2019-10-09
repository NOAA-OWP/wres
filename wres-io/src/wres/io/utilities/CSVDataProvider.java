package wres.io.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MissingValues;
import wres.util.Strings;
import wres.util.TimeHelper;

/**
 * A streaming tabular dataset that doesn't require an
 * active connection to a database. Mimics the behavior of
 * the <code>ResultSet</code> data structure used for
 * sql queries.
 */
class CSVDataProvider implements DataProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CSVDataProvider.class );

    private Map<String, Integer> columnNames;
    private int currentRow = -1;
    private boolean closed;
    private final File filePath;
    private BufferedReader reader;
    private String[] line = null;
    private final String delimiter;

    private CSVDataProvider( final File filePath, final String delimiter, final Map<String, Integer> columns) throws IOException
    {
        this.columnNames = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.delimiter = delimiter;
        this.filePath = filePath;

        if (columns != null)
        {
            this.columnNames.putAll( columns );
        }

        this.openFile();
    }

    static CSVDataProvider from( final String filePath, final String delimiter)
            throws IOException
    {
        return new CSVDataProvider( Paths.get( filePath).toFile(), delimiter, null);
    }

    static CSVDataProvider from( final URI filePath, final String delimiter)
            throws IOException
    {
        return new CSVDataProvider( Paths.get( filePath).toFile(), delimiter, null);
    }

    static CSVDataProvider from( final Path filePath, final String delimiter)
            throws IOException
    {
        return new CSVDataProvider( filePath.toFile(), delimiter, null);
    }

    static CSVDataProvider from( final String filePath, final String delimiter, final Map<String, Integer> columns)
            throws IOException
    {
        return new CSVDataProvider( Paths.get( filePath).toFile(), delimiter, columns);
    }

    static CSVDataProvider from( final URI filePath, final String delimiter, final Map<String, Integer> columns)
            throws IOException
    {
        return new CSVDataProvider( Paths.get( filePath).toFile(), delimiter, columns);
    }

    static CSVDataProvider from( final Path filePath, final String delimiter, final Map<String, Integer> columns)
            throws IOException
    {
        return new CSVDataProvider( filePath.toFile(), delimiter, columns);
    }

    private void openFile() throws IOException
    {
        FileReader fileReader = new FileReader( this.filePath );
        this.reader = new BufferedReader( fileReader );

        // If there aren't any columns defined, try to determine them from a possible header
        if (this.columnNames.isEmpty())
        {
            boolean dataExists = next();

            if (!dataExists)
            {
                throw new IOException( "There isn't any data to read in " + this.filePath.toPath() );
            }

            for (int i = 0; i < this.line.length; ++i)
            {
                this.columnNames.put( this.line[i], i );
            }
        }

        this.currentRow = -1;
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
        else if (this.line == null && this.currentRow >= 0)
        {
            throw new IndexOutOfBoundsException( "The position in the dataset may"
                                                 + " not move beyond the size of its data." );
        }

        String readLine;

        try
        {
            readLine = this.reader.readLine();
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "The CSV data was not in a state where more data could be read.", e );
        }

        if (readLine == null)
        {
            return false;
        }

        String[] futureLine = readLine.split( delimiter );

        for (int i = 0; i < futureLine.length; ++i)
        {
            futureLine[i] = futureLine[i].replaceAll( "(^\"|\"$)", "" );
        }

        if (this.columnNames.isEmpty())
        {
            this.line = futureLine;
        }
        else
        {
            this.line = new String[this.columnNames.size()];
            System.arraycopy( futureLine, 0, this.line, 0, Math.min( this.line.length, futureLine.length ) );
        }

        this.currentRow++;
        return true;
    }

    @Override
    public boolean back()
    {
        throw new UnsupportedOperationException( "The 'back' operation is not supported." );
    }

    @Override
    public void toEnd()
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The position in the data set may "
                                             + "not be moved to the end.");
        }

        while (this.next())
        {
            LOGGER.trace( "Moving to the end of the FileDataProvider for {}", this.filePath.toString() );
        }
    }

    @Override
    public void reset() throws IOException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The position in the data set may not "
                                             + "be moved back to its beginning.");
        }

        this.openFile();
    }

    @Override
    public boolean isNull( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        return this.getObject( this.getColumnIndex( columnName ) ) == null;
    }

    @Override
    public boolean hasColumn( String columnName )
    {
        return this.columnNames.containsKey( columnName );
    }

    @Override
    public boolean isEmpty()
    {
        return this.line == null;
    }

    @Override
    public Object[] getRowValues()
    {
        return this.line;
    }

    /**
     * Retrieves the value from the indicated row stored in the indicated column
     * @param index The index for the column containing the value
     * @return The value stored in the row and column
     */
    private Object getObject(int index)
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        // If "next" hasn't been called, go ahead and move on to the next row
        if (this.currentRow < 0)
        {
            this.next();
        }

        if (this.isEmpty())
        {
            throw new IndexOutOfBoundsException( "There is no data contained within the data set." );
        }

        return this.line[index];
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
            throw new IndexOutOfBoundsException( "There is no field in the data set named " + columnName );
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

        return this.getObject(this.getColumnIndex( columnName ));
    }

    @Override
    public boolean getBoolean( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The data set is inaccessible." );
        }

        Object value = this.getObject( columnName );

        return value != null && value.toString().contains( "1" );

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
                    "' in the field '" + columnName +
                    "' cannot be cast as a byte." );
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

        try
        {
            // If the string representation of whatever object can be used as a int, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Integer.parseInt( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type value '" + value.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as an integer." );
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

        try
        {
            // If the string representation of whatever object can be used as a short, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Short.parseShort( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The value '" + value.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as a short." );
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

        try
        {
            // If the string representation of whatever object can be used as a long, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Long.parseLong( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The type value '" + value.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as a long." );
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

        try
        {
            // If the string representation of whatever object can be used as a float, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Float.parseFloat( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The value '" + value.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as a float." );
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

        try
        {
            // If the string representation of whatever object can be used as a double, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Double.parseDouble( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The value '" + value.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as a double." );
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

        String arrayRepresentation = (String)array;

        // Remove all '(', ')', '{', '}', '[', and ']' characters
        arrayRepresentation = arrayRepresentation.replace( "(\\(|\\)|\\{|\\}|]][|\\])", "" );

        if (arrayRepresentation.isEmpty())
        {
            return new Double[0];
        }

        String[] numbers = arrayRepresentation.split( "," );
        Double[] result;
        try
        {
            result = new Double[numbers.length];

            for ( int i = 0; i < numbers.length; ++i )
            {
                result[i] = Double.parseDouble( numbers[i] );
            }
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The value '" + array.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as a double array." );
        }

        return result;
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

        String arrayRepresentation = (String)array;

        // Remove all '(', ')', '{', '}', '[', and ']' characters
        arrayRepresentation = arrayRepresentation.replace( "(\\(|\\)|\\{|\\}|]][|\\])", "" );

        if (arrayRepresentation.isEmpty())
        {
            return new Integer[0];
        }

        String[] numbers = arrayRepresentation.split( "," );
        Integer[] result;
        try
        {
            result = new Integer[numbers.length];

            for ( int i = 0; i < numbers.length; ++i )
            {
                result[i] = Integer.parseInt( numbers[i] );
            }
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The value '" + array.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as an integer array." );
        }

        return result;
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

        try
        {
            // If the string representation of whatever object can be used as a BigDecimal, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return new BigDecimal( value.toString() );
        }
        catch (NumberFormatException c)
        {
            throw new ClassCastException(
                    "The value '" + value.toString() +
                    "' in the field '" + columnName +
                    "' cannot be cast as a BigDecimal." );
        }
    }

    @Override
    public LocalTime getTime( String columnName )
    {
        if (this.isClosed())
        {
            throw new IllegalStateException( "The data set is inaccessible." );
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

        String actualValue = (String)value;
        String stringRepresentation = null;

        Instant result;

        if ( Strings.isNaturalNumber(actualValue))
        {
            result = Instant.ofEpochSecond( Long.parseLong( actualValue ) );
        }
        else if (Strings.isFloatingPoint( actualValue ))
        {
            Double epochSeconds = Double.parseDouble( actualValue );
            result = Instant.ofEpochSecond( epochSeconds.longValue() );
        }
        else
        {
            try
            {
                stringRepresentation = value.toString();
                stringRepresentation = stringRepresentation.replace( " ", "T" );

                if ( !stringRepresentation.endsWith( "Z" ) )
                {
                    stringRepresentation += "Z";
                }

                result = Instant.parse( stringRepresentation );
            }
            catch (DateTimeParseException e)
            {
                throw new IllegalStateException( "Neither '" + actualValue +
                                                 "' nor '" + stringRepresentation +
                                                 "' could be converted into an instant. If it really "
                                                 + "was supposed to be a date and time, ensure that it "
                                                 + "is in the format '1970-01-01T00:00:00Z'." );
            }
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

    @Override
    public <V> V getValue( String columnName )
    {
        throw new IllegalStateException("Types cannot be inferred from CSV data.");
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

        String uri = this.getString( columnName );

        if (uri == null)
        {
            return null;
        }

        return URI.create( uri );
    }

    @Override
    public void close()
    {
        this.closed = true;
        this.columnNames = null;

        try
        {
            this.reader.close();
        }
        catch ( IOException e )
        {
            LOGGER.warn("A CSV Data Provider could not be properly closed.");
        }

        this.currentRow = -1;
    }
}

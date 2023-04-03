package wres.io.reading.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MissingValues;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.data.DataProvider;
import wres.io.reading.ReaderUtilities;

/**
 * A streaming tabular dataset that doesn't require an
 * active connection to a database. Mimics the behavior of
 * the <code>ResultSet</code> data structure used for
 * sql queries.
 */
public class CsvDataProvider implements DataProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CsvDataProvider.class );

    private static final String DEFAULT_COMMENT_STRING = "#";
    private static final String THE_DATA_SET_IS_INACCESSIBLE = "The data set is inaccessible.";
    private static final String IN_THE_FIELD = "' in the field '";
    private static final String THE_VALUE = "The value '";
    private static final String TARGET = "(\\(|\\)|\\{|\\}|]][|\\])";

    private Map<String, Integer> columnNames;
    private int currentRow = -1;
    private boolean closed;
    private final Path filePath;
    private final BufferedReader reader;
    private String[] line = null;
    private final char delimiter;
    private final String commentString;

    /**
     * Creates an instance.
     * @param filePath the file path
     * @param delimiter the delimiter
     * @return the data provider
     * @throws IOException if the path could not be read
     */
    public static CsvDataProvider from( final String filePath, final char delimiter )
            throws IOException
    {
        return new CsvDataProvider( Paths.get( filePath ), delimiter, DEFAULT_COMMENT_STRING, null );
    }

    /**
     * Creates an instance.
     * @param inputStream the input stream
     * @param delimiter the delimiter
     * @return the data provider
     * @throws IOException if the path could not be read
     */
    public static CsvDataProvider from( final InputStream inputStream, final char delimiter )
            throws IOException
    {
        return new CsvDataProvider( inputStream, delimiter, DEFAULT_COMMENT_STRING, null );
    }

    /**
     * Creates an instance.
     * @param filePath the file path
     * @param delimiter the delimiter
     * @return the data provider
     * @throws IOException if the path could not be read
     */

    public static CsvDataProvider from( final URI filePath, final char delimiter )
            throws IOException
    {
        return new CsvDataProvider( Paths.get( filePath ), delimiter, DEFAULT_COMMENT_STRING, null );
    }

    /**
     * Creates an instance.
     * @param filePath the file path
     * @param delimiter the delimiter
     * @param columns the columns
     * @return the data provider
     * @throws IOException if the path could not be read
     */

    public static CsvDataProvider from( final URI filePath, final char delimiter, final Map<String, Integer> columns )
            throws IOException
    {
        return new CsvDataProvider( Paths.get( filePath ), delimiter, DEFAULT_COMMENT_STRING, columns );
    }

    private void openFile() throws IOException
    {
        // If there aren't any columns defined, try to determine them from a possible header
        if ( this.columnNames.isEmpty() )
        {
            boolean dataExists = next();

            if ( !dataExists )
            {
                throw new IOException( "There isn't any data to read in the file or stream provided (the file was "
                                       + this.filePath
                                       + ")." );
            }

            for ( int i = 0; i < this.line.length; ++i )
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
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The position in the dataset "
                                             + "may not be incremented." );
        }
        else if ( this.line == null && this.currentRow >= 0 )
        {
            throw new IndexOutOfBoundsException( "The position in the dataset may"
                                                 + " not move beyond the size of its data." );
        }

        String readLine;

        try
        {
            // #108359
            while ( Objects.nonNull( readLine = this.reader.readLine() )
                    && readLine.startsWith( this.commentString ) )
            {
                // Continue to skip comment lines
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "The CSV data was not in a state where more data could be read.", e );
        }

        if ( readLine == null )
        {
            return false;
        }

        String[] futureLine = ReaderUtilities.splitByDelimiter( readLine, this.delimiter );

        if ( this.columnNames.isEmpty() )
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
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The position in the data set may "
                                             + "not be moved to the end." );
        }

        while ( this.next() )
        {
            LOGGER.trace( "Moving to the end of the FileDataProvider for {}.", this.filePath );
        }
    }

    @Override
    public void reset() throws IOException
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The position in the data set may not "
                                             + "be moved back to its beginning." );
        }

        this.openFile();
    }

    @Override
    public boolean isNull( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
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
    private Object getObject( int index )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        // If "next" hasn't been called, go ahead and move on to the next row
        if ( this.currentRow < 0 )
        {
            this.next();
        }

        if ( this.isEmpty() )
        {
            throw new IndexOutOfBoundsException( "There is no data contained within the data set." );
        }

        return this.line[index];
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
            throw new IndexOutOfBoundsException( "There is no field in the data set named " + columnName );
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
    public int getRowIndex()
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        return this.currentRow;
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

        return this.getObject( this.getColumnIndex( columnName ) );
    }

    @Override
    public boolean getBoolean( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object value = this.getObject( columnName );

        return value != null && value.toString().contains( "1" );

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

        try
        {
            // If the string representation of whatever object can be used as a byte, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Byte.parseByte( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    "The type '" + value.getClass().toString()
                    +
                    "' with the value '"
                    + value
                    + IN_THE_FIELD
                    + columnName
                    +
                    "' cannot be cast as a byte." );
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

        try
        {
            // If the string representation of whatever object can be used as a int, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Integer.parseInt( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    "The type value '" + value.toString()
                    +
                    IN_THE_FIELD
                    + columnName
                    +
                    "' cannot be cast as an integer." );
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

        try
        {
            // If the string representation of whatever object can be used as a short, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Short.parseShort( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_VALUE + value
                                          + IN_THE_FIELD
                                          + columnName
                                          +
                                          "' cannot be cast as a short." );
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

        try
        {
            // If the string representation of whatever object can be used as a long, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Long.parseLong( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    "The type value '" + value.toString()
                    +
                    IN_THE_FIELD
                    + columnName
                    +
                    "' cannot be cast as a long." );
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

        try
        {
            // If the string representation of whatever object can be used as a float, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Float.parseFloat( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    THE_VALUE + value.toString()
                    +
                    IN_THE_FIELD
                    + columnName
                    +
                    "' cannot be cast as a float." );
        }
    }

    @Override
    public double getDouble( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        String value = this.getString( columnName );

        if ( value == null || value.isBlank() )
        {
            return MissingValues.DOUBLE;
        }

        try
        {
            // If the string representation of whatever object can be used as a double, use that
            // This should work in cases where the value is "1.0" or an object whose "toString()" returns a number.
            return Double.parseDouble( value );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException( THE_VALUE + value
                                          +
                                          IN_THE_FIELD
                                          + columnName
                                          +
                                          "' cannot be cast as a double." );
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

        String arrayRepresentation = ( String ) array;

        // Remove all '(', ')', '{', '}', '[', and ']' characters
        arrayRepresentation = arrayRepresentation.replace( TARGET, "" );

        if ( arrayRepresentation.isEmpty() )
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
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    THE_VALUE + array.toString()
                    +
                    IN_THE_FIELD
                    + columnName
                    +
                    "' cannot be cast as a double array." );
        }

        return result;
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

        String arrayRepresentation = ( String ) array;

        // Remove all '(', ')', '{', '}', '[', and ']' characters
        arrayRepresentation = arrayRepresentation.replace( TARGET, "" );

        if ( arrayRepresentation.isEmpty() )
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
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    THE_VALUE + array.toString()
                    +
                    IN_THE_FIELD
                    + columnName
                    +
                    "' cannot be cast as an integer array." );
        }

        return result;
    }

    @Override
    public String[] getStringArray( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Object array = this.getObject( columnName );

        if ( array == null )
        {
            return new String[0];
        }

        String arrayRepresentation = ( String ) array;

        // Remove all '(', ')', '{', '}', '[', and ']' characters
        arrayRepresentation = arrayRepresentation.replace( TARGET, "" );

        if ( arrayRepresentation.isEmpty() )
        {
            return new String[0];
        }

        return arrayRepresentation.split( "," );
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

        try
        {
            // If the string representation of whatever object can be used as a BigDecimal, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return new BigDecimal( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    THE_VALUE + value.toString()
                    +
                    IN_THE_FIELD
                    + columnName
                    +
                    "' cannot be cast as a BigDecimal." );
        }
    }

    @Override
    public LocalTime getTime( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
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

        CharSequence value = ( CharSequence ) this.getObject( columnName );
        return Instant.parse( value );
    }

    public Duration getDuration( String columnName )
    {
        Duration result;

        Object value = this.getObject( columnName );

        if ( value == null )
        {
            return null;
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
                                                columnName
                                                +
                                                "' cannot be converted into a Duration." );
        }

        return result;
    }

    @Override
    public <V> V getValue( String columnName )
    {
        throw new IllegalStateException( "Types cannot be inferred from CSV data." );
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

        String uri = this.getString( columnName );

        if ( uri == null )
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
            LOGGER.warn( "A CSV Data Provider could not be properly closed." );
        }

        this.currentRow = -1;
    }

    private CsvDataProvider( Path filePath,
                             char delimiter,
                             String commentString,
                             Map<String, Integer> columns )
            throws IOException
    {
        this.columnNames = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.delimiter = delimiter;
        this.filePath = filePath;
        this.commentString = commentString;

        if ( columns != null )
        {
            this.columnNames.putAll( columns );
        }

        this.reader = Files.newBufferedReader( this.filePath, StandardCharsets.UTF_8 );

        this.openFile();
    }

    private CsvDataProvider( InputStream inputStream,
                             char delimiter,
                             String commentString,
                             Map<String, Integer> columns )
            throws IOException
    {
        this.columnNames = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.delimiter = delimiter;
        this.filePath = null;
        this.commentString = commentString;

        if ( columns != null )
        {
            this.columnNames.putAll( columns );
        }

        this.reader = new BufferedReader( new InputStreamReader( inputStream, StandardCharsets.UTF_8 ) );

        this.openFile();
    }
}

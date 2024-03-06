package wres.reading.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MissingValues;
import wres.reading.ReaderUtilities;

/**
 * A streaming tabular dataset that doesn't require an active connection to a database. Mimics the behavior of the
 * <code>ResultSet</code> data structure used for sql queries.
 */
class CsvDataProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CsvDataProvider.class );
    private static final String DEFAULT_COMMENT_STRING = "#";
    private static final String THE_DATA_SET_IS_INACCESSIBLE = "The data set is inaccessible.";
    private static final String IN_THE_FIELD = "' in the field '";
    private static final String THE_VALUE = "The value '";
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

    public boolean isClosed()
    {
        return this.closed;
    }

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

    public boolean back()
    {
        throw new UnsupportedOperationException( "The 'back' operation is not supported." );
    }

    public boolean isNull( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        return this.getObject( this.getColumnIndex( columnName ) ) == null;
    }

    public boolean hasColumn( String columnName )
    {
        return this.columnNames.containsKey( columnName );
    }

    public boolean isEmpty()
    {
        return this.line == null;
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

    public List<String> getColumnNames()
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( "The dataset is not accessible." );
        }
        return new ArrayList<>( columnNames.keySet() );
    }

    public int getRowIndex()
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        return this.currentRow;
    }

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
            // If the string representation of whatever object can be used as an int, use that
            // This should work in cases where the value is "1" or an object whose "toString()" returns a number.
            return Integer.parseInt( value.toString() );
        }
        catch ( NumberFormatException c )
        {
            throw new ClassCastException(
                    "The type value '" + value
                    + IN_THE_FIELD
                    + columnName
                    + "' cannot be cast as an integer." );
        }
    }

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
                    "The type value '" + value
                    + IN_THE_FIELD
                    + columnName
                    + "' cannot be cast as a long." );
        }
    }

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

    public LocalTime getTime( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        Instant instant = this.getInstant( columnName );
        return instant.atOffset( ZoneOffset.UTC ).toLocalTime();
    }

    public Instant getInstant( String columnName )
    {
        if ( this.isClosed() )
        {
            throw new IllegalStateException( THE_DATA_SET_IS_INACCESSIBLE );
        }

        CharSequence value = ( CharSequence ) this.getObject( columnName );
        return Instant.parse( value );
    }

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

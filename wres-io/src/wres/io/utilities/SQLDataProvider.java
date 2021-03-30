package wres.io.utilities;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MissingValues;
import wres.util.TimeHelper;

/**
 * A {@link DataProvider} that provides buffered access to the results of a database call
 */
public class SQLDataProvider implements DataProvider
{
    private static final String THE_DATA_IS_NOT_ACCESSIBLE = "The data is not accessible.";

    private static final Logger LOGGER = LoggerFactory.getLogger( SQLDataProvider.class );

    /**
     * The connection to the database that returned this data. Must be kept open for the
     * duration of the DataProvider to keep open access to the underlying {@link ResultSet} that
     * provides all of the data
     */
    private final Connection connection;

    /**
     * The database statement that ran the query that retrieved these results. Must be kept
     * open for the duration of the DataProvider to keep open access to the underlying
     * {@link ResultSet} that provides all of the data
     */
    private final Statement statement;

    /**
     * The raw results from a database call
     */
    private final ResultSet resultSet;

    /**
     * The list of the column names in the results
     */
    private final List<String> columnNames;

    /**
     * Helpful flag for detecting whether or not all resources have been closed
     */
    private boolean closed = false;

    /**
     * Constructor
     * @param connection The connection to where this data came from
     * @param resultSet The data streaming through the connection
     */
    SQLDataProvider(final Connection connection, final ResultSet resultSet)
    {
        Objects.requireNonNull( connection );
        Objects.requireNonNull( resultSet );

        LOGGER.trace( "Created {} with connection {}", this, connection );

        try
        {
            // Go ahead and throw an error if we can't access the data
            if (resultSet.isClosed())
            {
                throw new IllegalStateException( "The given resultset has already been closed." );
            }

            // Store the objects that helped retrieve the data so that the connection isn't closed
            // when the instances are destroyed
            this.connection = connection;
            this.statement = resultSet.getStatement();
            this.resultSet = resultSet;

            // Record the column names to support metadata operations without digging through the result set
            this.columnNames = new ArrayList<>();
            int columnCount = this.resultSet.getMetaData().getColumnCount();

            // ResultSets are 1's indexed
            for (int index = 1; index <= columnCount; ++index)
            {
                this.columnNames.add( resultSet.getMetaData().getColumnLabel( index ));
            }
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The data set is inaccessible.", e );
        }
    }

    @Override
    public boolean isClosed()
    {
        // If we've already acknowledged that the connection is closed, we don't have to do anything,
        // otherwise we need to consult the result set to be sure
        if (!this.closed)
        {
            try
            {
                this.closed = this.resultSet.isClosed();
            }
            catch ( SQLException e )
            {
                LOGGER.warn( "The data set is inaccessible.", e );
                this.closed = true;
            }
        }

        return this.closed;
    }

    @Override
    public boolean next()
    {
        try
        {
            return this.resultSet.next();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The position in the dataset "
                                             + "may not be incremented.",
                                             e );
        }
    }

    @Override
    public boolean back()
    {
        try
        {
            return this.resultSet.previous();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The position in the data set "
                                             + "may not be decremented.",
                                             e );
        }
    }

    @Override
    public void toEnd()
    {
        try
        {
            this.resultSet.last();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The position in the data set may "
                                             + "not be moved to the end.",
                                             e );
        }
    }

    @Override
    public void reset()
    {
        try
        {
            this.resultSet.first();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The position in the data set may not "
                                             + "be moved back to its beginning.",
                                             e );
        }
    }

    @Override
    public int getColumnIndex( String columnName )
    {
        // Find the string, ignoring case
        for( int i = 0; i < this.columnNames.size(); i++ )
        {
            if( this.columnNames.get( i ).equalsIgnoreCase( columnName ) )
            {
                return i;
            }
        }
        
        return -1;
    }

    @Override
    public List<String> getColumnNames()
    {
        return this.columnNames;
    }

    @Override
    public int getRowIndex()
    {
        try
        {
            return this.resultSet.getRow();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public boolean isNull( String columnName )
    {
        return Objects.isNull( this.getObject( columnName ) );
    }

    @Override
    public boolean hasColumn( String columnName )
    {
        return this.columnNames.stream()
                               .anyMatch( next -> next.equalsIgnoreCase( columnName ) );
    }

    @Override
    public boolean isEmpty()
    {
        try
        {
            // If `getRow` is 1 or more, that means that there is at least one value, and `isBeforeFirst`
            // is only true if it is before the first value. The combination means that it hasn't moved on
            // to a row of data AND nothing lies after the first value
            return this.resultSet.getRow() == 0 && !this.resultSet.isBeforeFirst();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Object[] getRowValues()
    {
        ArrayList<Object> values = new ArrayList<>(  );

        for (String columnName : this.getColumnNames())
        {
            Object value = this.getObject( columnName );

            if (value == null)
            {
                values.add(null);
            }
            else if (value instanceof Array )
            {
                values.add(this.getArray( columnName ));
            }
            else if (value instanceof Time)
            {
                values.add(this.getTime( columnName ));
            }
            else if (value instanceof Date)
            {
                values.add(this.getDate( columnName ));
            }
            else if (value instanceof Timestamp)
            {
                values.add(this.getInstant( columnName ));
            }
            else
            {
                values.add( this.getObject( columnName ) );
            }
        }

        return values.toArray( new Object[0] );
    }

    /**
     * Converts the value of a ResultSet Array into a usable object
     * <br><br>
     *     <p>
     *         An array in a {@link ResultSet} is just a wrapper for the actual array data. This gets the inner data.
     *     </p>
     * @param columnName The name of the column that should contain array data
     * @return An object that may be used as a Java (vs. JDBC) array
     */
    private Object getArray(final String columnName)
    {
        try
        {
            // getArray nets a JDBC Array object, so we go one step further to get the actual data
            return this.resultSet.getArray( columnName ).getArray();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Object getObject( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getObject( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public boolean getBoolean( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getBoolean( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public String getString( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getString( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public URI getURI( String columnName)
    {
        String possibleURI;
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }

            possibleURI = this.resultSet.getString( columnName );

            if (possibleURI == null)
            {
                return null;
            }
            else
            {
                return URI.create( possibleURI );
            }
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Byte getByte( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }

            return this.resultSet.getByte( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Integer getInt( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getInt( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Short getShort( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getShort( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Long getLong( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getLong( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Float getFloat( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getFloat( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public double getDouble( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            
            double returnMe = this.resultSet.getDouble( columnName );
            
            // Return Double.NaN if the value was SQL NULL
            if( this.resultSet.wasNull() )
            {
                return MissingValues.DOUBLE;
            }
            
            return returnMe;
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public Double[] getDoubleArray( String columnName )
    {
        Array outer = null;
        Double[] rawResult;

        try
        {
            // Probe the inner type of array before casting
            // See #56214-139-140
            outer = this.resultSet.getArray( columnName );
            Object inner = outer.getArray();

            if ( inner instanceof Double[] )
            {
                rawResult = (Double[]) inner;
            }
            else if ( inner instanceof Object[] )
            {
                Object[] toTransform = (Object[]) inner;
                rawResult = Arrays.copyOf( toTransform, toTransform.length, Double[].class );
            }
            else
            {
                throw new ClassCastException( "Could not cast the input type of '"
                                              + inner.getClass()
                                              + "' with SQL type '"
                                              + outer.getBaseTypeName()
                                              + "' to a Double[]." );
            }
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
        finally
        {
            // Unfortunately, Closeable was not used in JDBC
            if ( Objects.nonNull( outer ) )
            {
                try
                {
                    outer.free();
                }
                catch ( SQLException se )
                {
                    LOGGER.warn( "Could not release resources for column {} in {}",
                                 columnName, this.resultSet );
                }
            }
        }

        // Translate NULL in wres DB to missing value in wres Java
        return Arrays.stream( rawResult )
                     .map( d -> Objects.isNull( d ) ? MissingValues.DOUBLE : d )
                     .toArray( Double[]::new );
    }

    @Override
    public Integer[] getIntegerArray(String columnName)
    {
        try
        {
            return (Integer[])this.resultSet.getArray( columnName ).getArray();
        }
        catch (SQLException e)
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public String[] getStringArray(String columnName)
    {
        Array outer = null;
        String[] rawResult;

        try
        {
            // Probe the inner type of array before casting
            // See #56214-139-140
            outer = this.resultSet.getArray( columnName );
            Object inner = outer.getArray();

            if ( inner instanceof Double[] )
            {
                rawResult = (String[]) inner;
            }
            else if ( inner instanceof Object[] )
            {
                Object[] toTransform = (Object[]) inner;
                rawResult = Arrays.copyOf( toTransform, toTransform.length, String[].class );
            }
            else
            {
                throw new ClassCastException( "Could not cast the input type of '"
                                              + inner.getClass()
                                              + "' with SQL type '"
                                              + outer.getBaseTypeName()
                                              + "' to a String[]." );
            }
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
        finally
        {
            // Unfortunately, Closeable was not used in JDBC
            if ( Objects.nonNull( outer ) )
            {
                try
                {
                    outer.free();
                }
                catch ( SQLException se )
                {
                    LOGGER.warn( "Could not release resources for column {} in {}",
                                 columnName, this.resultSet );
                }
            }
        }
        
        return rawResult;
    }
    
    @Override
    public BigDecimal getBigDecimal( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getBigDecimal( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public LocalTime getTime( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }

            if (resultSet.getObject( columnName ) instanceof java.sql.Time)
            {
                return this.resultSet.getTime( columnName ).toLocalTime();
            }
            else if (resultSet.getObject( columnName ) instanceof java.sql.Timestamp)
            {
                return this.resultSet.getTimestamp( columnName ).toLocalDateTime().toLocalTime();
            }

            Instant instant = this.getInstant( columnName );
            return instant.atOffset( ZoneOffset.UTC ).toLocalTime();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public LocalDate getDate( String columnName )
    {
        try
        {
            // Jump to the first row if the jump hasn't already been made
            if (this.resultSet.isBeforeFirst())
            {
                this.resultSet.next();
            }
            return this.resultSet.getDate( columnName ).toLocalDate();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public OffsetDateTime getOffsetDateTime( String columnName )
    {
        Instant instant = this.getInstant( columnName );

        if (instant == null)
        {
            return null;
        }

        return OffsetDateTime.ofInstant( instant, ZoneId.of( "UTC" ) );
    }

    @Override
    public LocalDateTime getLocalDateTime( String columnName )
    {
        return LocalDateTime.ofInstant( this.getInstant( columnName ), ZoneId.of("UTC"));
    }

    @Override
    public Instant getInstant( String columnName )
    {
        Instant result;

        try
        {
            // Jump to the first row if the jump hasn't already been made
            if ( resultSet.isBeforeFirst() )
            {
                resultSet.next();
            }

            // OffsetDateTime gives consistent results for both h2 and postgres.
            OffsetDateTime resultObject = resultSet.getObject( columnName,
                                                               OffsetDateTime.class );
            result = resultObject.toInstant();
            LOGGER.debug( "getInstant( '{}' ) turned OffsetDateTime '{}' into Instant '{}'",
                          columnName, resultObject, result );
        }
        catch (SQLException e)
        {
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }

        return result;
    }

    @Override
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
            // If the returned number was somewhat numerical, we're going to treat it as the number of
            // units in our resolution. Since there's no other information to go by, we just need to
            // assume the lead resolution of the application
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
        try
        {
            if ( this.resultSet.isBeforeFirst() )
            {
                this.resultSet.next();
            }
            return ( V ) this.resultSet.getObject( columnName );
        }
        catch ( SQLException e )
        {
            // We don't care about this error; it occurs if there
            // was an issue on the database side, not application side
            throw new IllegalStateException( THE_DATA_IS_NOT_ACCESSIBLE, e );
        }
    }

    @Override
    public void close()
    {
        if (!this.isClosed())
        {
            try
            {
                this.resultSet.close();
            }
            catch ( SQLException e )
            {
                LOGGER.warn( "A ResultSet could not be properly closed.", e );
            }

            try
            {
                this.statement.close();
            }
            catch ( SQLException e )
            {
                LOGGER.warn( "A Statement for a ResultSet could not be closed.",
                             e );
            }

            try
            {
                // Query class and this class have an informal agreement that
                // SQLDataProvider will setAutoCommit( true ) before closing
                // because we don't want to change the state while the ResultSet
                // is still open. Might be unnecessary due to DatabaseSettings
                // "setAutoCommitOnClose( true )" on the pool/DataSource.
                LOGGER.trace( "Setting autocommit to true on {} in {}",
                              this.connection, this );
                this.connection.setAutoCommit( true );
            }
            catch ( SQLException se )
            {
                LOGGER.warn( "Unable to set connection " + connection +
                             " to autocommit=true", se );
            }

            try
            {
                LOGGER.trace( "Closing connection {} in {}",
                              this.connection, this );

                this.connection.close();

                if ( this.connection.isClosed() )
                {
                    LOGGER.trace( "The connection {} in {} successfully closed.",
                                 this.connection, this );
                }
                else
                {
                    LOGGER.error( "The connection {} in {} wasn't closed.",
                                  this.connection, this );
                }
            }
            catch ( SQLException e )
            {
                LOGGER.warn( "The connection for a ResultSet could not be closed.",
                             e );
            }
        }
        else
        {
            LOGGER.debug("This {} data set is already closed.", this );
        }
    }
}

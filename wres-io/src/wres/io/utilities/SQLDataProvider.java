package wres.io.utilities;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLDataProvider implements DataProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SQLDataProvider.class );
    private final ResultSet resultSet;
    private final List<String> columnNames;
    private boolean closed = false;

    SQLDataProvider( final ResultSet resultSet)
    {
        try
        {
            if (resultSet.isClosed())
            {
                throw new IllegalStateException( "The given resultset has already been closed." );
            }
            this.resultSet = resultSet;

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
        if (!this.closed)
        {
            try
            {
                this.closed = this.resultSet.isClosed();
            }
            catch ( SQLException e )
            {
                LOGGER.error( "The data set is inaccessible.", e );
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
        Integer columnIndex = -1;

        try
        {
            int columnCount = this.resultSet.getMetaData().getColumnCount();
            for ( int index = 1; index <= columnCount; ++index )
            {
                if ( this.resultSet.getMetaData().getColumnLabel( index ).equals( columnName ) )
                {
                    // Subtract by 1 to compensate for 1's indexing;
                    // The first column should be index 0, not 1
                    columnIndex = index - 1;
                    break;
                }
            }
        }
        catch (SQLException e)
        {
            throw new IllegalStateException( "The dataset is not accessible.", e );
        }

        return columnIndex;
    }

    @Override
    public Iterable<String> getColumnNames()
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
            throw new IllegalStateException( "The data is not accessible.", e );
        }
    }

    @Override
    public boolean isNull( String columnName )
    {
        return this.getObject( columnName ) == null;
    }

    @Override
    public boolean hasColumn( String columnName )
    {
        return this.columnNames.contains( columnName );
    }

    @Override
    public boolean isEmpty()
    {
        try
        {
            return this.resultSet.isAfterLast();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The data is not accessible.", e );
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

        return values.toArray( new Object[values.size()] );
    }

    private Object getArray(final String columnName)
    {
        try
        {
            return this.resultSet.getArray( columnName ).getArray();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The data is not accessible.", e );
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
            throw new IllegalStateException( "The data is not accessible.", e );
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
            throw new IllegalStateException( "The data is not accessible.", e );
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
            throw new IllegalStateException( "The data is not accessible.", e );
        }
    }

    @Override
    public byte getByte( String columnName )
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
            throw new IllegalStateException( "The data is not accessible.", e );
        }
    }

    @Override
    public int getInt( String columnName )
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
            throw new IllegalStateException( "The data is not accessible.", e );
        }
    }

    @Override
    public short getShort( String columnName )
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
            throw new IllegalStateException( "The data is not accessible.", e );
        }
    }

    @Override
    public long getLong( String columnName )
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
            throw new IllegalStateException( "The data is not accessible.", e );
        }
    }

    @Override
    public float getFloat( String columnName )
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
            throw new IllegalStateException( "The data is not accessible.", e );
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
            return this.resultSet.getDouble( columnName );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The data is not accessible.", e );
        }
    }

    @Override
    public double[] getDoubleArray( String columnName )
    {
        try
        {
            return (double[])this.resultSet.getArray( columnName ).getArray();
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "The data is not accessible.", e );
        }
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
            throw new IllegalStateException( "The data is not accessible.", e );
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
            throw new IllegalStateException( "The data is not accessible.", e );
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
            throw new IllegalStateException( "The data is not accessible.", e );
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

            // Timestamps are interpretted as strings in order to avoid the 'help'
            // that JDBC provides by converting timestamps to local times and
            // applying daylight savings changes
            if ( resultSet.getObject( columnName ) instanceof String ||
                 resultSet.getObject( columnName ) instanceof Timestamp )
            {
                String stringRepresentation = resultSet.getString( columnName );
                stringRepresentation = stringRepresentation.replace( " ", "T" );

                if ( !stringRepresentation.endsWith( "Z" ) )
                {
                    stringRepresentation += "Z";
                }

                result = Instant.parse( stringRepresentation );
            }
            else if ( resultSet.getObject( columnName ) instanceof Integer )
            {
                result = Instant.ofEpochSecond( resultSet.getInt( columnName ) );
            }
            else if ( resultSet.getObject( columnName ) instanceof Long )
            {
                result = Instant.ofEpochSecond( resultSet.getLong( columnName ) );
            }
            else if ( resultSet.getObject( columnName ) instanceof Double )
            {
                Double epochSeconds = ( Double ) resultSet.getObject( columnName );
                result = Instant.ofEpochSecond( epochSeconds.longValue() );
            }
            else
            {
                throw new IllegalStateException( "The column type for '" +
                                                 columnName +
                                                 "' (value = " +
                                                 resultSet.getObject( columnName ).toString() +
                                                 ") cannot be converted into an Instant." );
            }
        }
        catch (SQLException e)
        {
            throw new IllegalStateException( "The data is not accessible.", e );
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
            throw new IllegalStateException( "The data is not accessible.", e );
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
                LOGGER.debug("The contained resultset could not be properly closed.", e);
            }

            try
            {
                this.resultSet.getStatement().close();
            }
            catch ( SQLException e )
            {
                LOGGER.error("The statement for the result set could not be closed.");
            }
        }
        else
        {
            LOGGER.debug("This data set is already closed.");
        }
    }
}

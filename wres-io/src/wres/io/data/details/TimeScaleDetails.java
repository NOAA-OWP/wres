package wres.io.data.details;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScaleOuter;
import wres.io.data.DataProvider;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Represents a TimeScale row from the database.
 * Based on FeatureDetails.
 */

public class TimeScaleDetails extends CachedDetail<TimeScaleDetails, TimeScaleOuter>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeScaleDetails.class );
    private static final long PLACEHOLDER_ID = Long.MIN_VALUE;

    private long id = PLACEHOLDER_ID;
    private TimeScaleOuter key = null;

    /**
     * Creates an instance.
     * @param key the key
     */
    public TimeScaleDetails( TimeScaleOuter key )
    {
        this.key = key;
    }

    @Override
    protected void update( DataProvider row )
    {
        Long durationInMillis = row.getLong( "duration_ms" );
        String functionRaw = row.getValue( "function_Name" );

        Duration duration = null;

        if ( durationInMillis != null )
        {
            duration = Duration.ofMillis( durationInMillis );
        }

        TimeScaleFunction function = TimeScaleFunction.valueOf( functionRaw );
        this.key = TimeScaleOuter.of( duration, function );

        if ( row.hasColumn( this.getIDName() ) )
        {
            this.setID( row.getLong( this.getIDName() ) );
        }
    }

    @Override
    public int compareTo( @NotNull TimeScaleDetails other )
    {
        if ( this.equals( other ) )
        {
            return 0;
        }

        if ( this.id == PLACEHOLDER_ID
             && other.id == PLACEHOLDER_ID )
        {
            return this.getKey().compareTo( other.getKey() );
        }

        int idComparison = Long.compare( this.id, other.id );

        if ( idComparison != 0 )
        {
            return idComparison;
        }

        return this.getKey().compareTo( other.getKey() );
    }

    @Override
    public TimeScaleOuter getKey()
    {
        return this.key;
    }

    @Override
    public Long getId()
    {
        return this.id;
    }

    @Override
    protected String getIDName()
    {
        return "timescale_id";
    }

    @Override
    public void setID( long id )
    {
        this.id = id;
    }

    @Override
    protected Logger getLogger()
    {
        return TimeScaleDetails.LOGGER;
    }

    @Override
    protected DataScripter getInsertSelect( Database database )
    {
        DataScripter script = new DataScripter( database );
        this.addInsert( script );
        script.setUseTransaction( true );

        script.retryOnSerializationFailure();
        script.retryOnUniqueViolation();

        script.setHighPriority( true );
        return script;
    }

    private void addInsert( final DataScripter script )
    {
        Duration duration = this.getKey()
                                .getPeriod();
        Long durationInMillis = null;

        if ( duration != null )
        {
            durationInMillis = duration.toMillis();
        }

        String functionRaw = this.getKey()
                                 .getFunction()
                                 .toString();

        script.addLine( "INSERT INTO wres.TimeScale ( duration_ms, function_name ) " );
        script.addTab().addLine( "SELECT ?, ?" );

        script.addArgument( durationInMillis );
        script.addArgument( functionRaw );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( "SELECT 1" );
        script.addTab( 2 ).addLine( "FROM wres.TimeScale" );
        script.addTab( 2 ).addLine( "WHERE function_name = ?" );
        script.addArgument( functionRaw );

        if ( Objects.isNull( durationInMillis ) )
        {
            script.addTab( 3 ).addLine( "AND duration_ms IS NULL" );
        }
        else
        {
            script.addTab( 3 ).addLine( "AND duration_ms = ?" );
            script.addArgument( durationInMillis );
        }

        script.addTab().addLine( ")" );
    }

    private void addSelect( final DataScripter script )
    {

        Duration duration = this.getKey()
                                .getPeriod();
        Long durationInMillis = null;

        if ( duration != null )
        {
            durationInMillis = duration.toMillis();
        }

        String functionRaw = this.getKey()
                                 .getFunction()
                                 .toString();

        script.addLine( "SELECT timescale_id, duration_ms, function_Name" );
        script.addLine( "FROM wres.TimeScale" );
        script.addTab( 2 ).addLine( "WHERE function_Name = ?" );
        script.addArgument( functionRaw );

        if ( Objects.isNull( durationInMillis ) )
        {
            script.addTab( 3 ).addLine( "AND duration_ms IS NULL" );
        }
        else
        {
            script.addTab( 3 ).addLine( "AND duration_ms = ?" );
            script.addArgument( durationInMillis );
        }

        script.setMaxRows( 1 );
    }

    @Override
    public void save( Database database ) throws SQLException
    {
        DataScripter script = this.getInsertSelect( database );
        boolean performedInsert = script.execute() > 0;

        if ( performedInsert )
        {
            this.id = script.getInsertedIds()
                            .get( 0 );
        }
        else
        {
            DataScripter scriptWithId = new DataScripter( database );
            scriptWithId.setHighPriority( true );

            // The insert has already happened, we are in the same thread, so
            // there should be no need to serialize here, right?
            scriptWithId.setUseTransaction( false );
            this.addSelect( scriptWithId );

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.update( data );
            }
        }

        LOGGER.trace( "Did I create TimeScale ID {}? {}",
                      this.id,
                      performedInsert );
    }

    @Override
    protected Object getSaveLock()
    {
        return new Object();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "id", id )
                .append( "key", key )
                .toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TimeScaleDetails that = ( TimeScaleDetails ) o;
        return id == that.id &&
               Objects.equals( key, that.key );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( id, key );
    }

}

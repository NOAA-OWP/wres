package wres.io.data.details;

import java.sql.SQLException;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.space.FeatureKey;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Defines the important details of a feature as stored in the database
 * @author Christopher Tubbs
 */
public class FeatureDetails extends CachedDetail<FeatureDetails, FeatureKey>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDetails.class);
    private static final long PLACEHOLDER_ID = Long.MIN_VALUE;

    private long id = PLACEHOLDER_ID;
	private FeatureKey key = null;

	public FeatureDetails()
    {
        super();
    }

    public FeatureDetails( FeatureKey key)
    {
        this.key = key;
    }

    public FeatureDetails( DataProvider row )
    {
        this.update( row );
    }

    @Override
    protected void update( DataProvider row )
    {
        String name = row.getValue( "name" );
        String description = row.getValue( "description" );
        Integer srid = row.getValue( "srid" );
        String wkt = row.getValue( "wkt" );

        this.key = new FeatureKey( name, description, srid, wkt );

        if (row.hasColumn(this.getIDName() ))
        {
            this.setID( row.getLong( this.getIDName() ) );
        }
    }

	@Override
	public int compareTo( FeatureDetails other )
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
	public FeatureKey getKey()
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
		return "feature_id";
	}

	@Override
	public void setID( long id )
	{
		this.id = id;
	}

    @Override
    protected Logger getLogger()
    {
        return FeatureDetails.LOGGER;
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

    private void addInsert(final DataScripter script)
    {
        String name = this.getKey()
                          .getName();
        String description = this.getKey()
                                 .getDescription();
        Integer srid = this.getKey()
                           .getSrid();
        String wkt = this.getKey()
                         .getWkt();

        script.addLine( "INSERT INTO wres.Feature ( name, description, srid, wkt ) ");
        script.addTab().addLine( "SELECT ?, ?, ?, ?" );

        script.addArgument( name );
        script.addArgument( description );
        script.addArgument( srid );
        script.addArgument( wkt );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( "SELECT 1" );
        script.addTab( 2 ).addLine( "FROM wres.Feature" );
        script.addTab( 2 ).addLine( "WHERE name = ?");
        script.addArgument( name );

        if ( Objects.isNull( description ) )
        {
            script.addTab(  3  ).addLine( "AND description IS NULL" );
        }
        else
        {
            script.addTab(  3  ).addLine( "AND description = ?");
            script.addArgument( description );
        }

        if ( Objects.isNull( srid ) )
        {
            script.addTab(  3  ).addLine( "AND srid IS NULL" );
        }
        else
        {
            script.addTab(  3  ).addLine( "AND srid = ? ");
            script.addArgument( srid );
        }

        if ( Objects.isNull( wkt ) )
        {
            script.addTab(  3  ).addLine( "AND wkt IS NULL" );
        }
        else
        {
            script.addTab(  3  ).addLine( "AND wkt = ? ");
            script.addArgument( wkt );
        }

        script.addTab().addLine(")");
    }

    private void addSelect(final DataScripter script)
    {
        String name = this.getKey()
                          .getName();
        String description = this.getKey()
                                 .getDescription();
        Integer srid = this.getKey()
                           .getSrid();
        String wkt = this.getKey()
                         .getWkt();

        script.addLine("SELECT feature_id, name, description, srid, wkt");
        script.addLine("FROM wres.Feature");
        script.addTab( 2 ).addLine( "WHERE name = ? ");
        script.addArgument( name );

        if ( Objects.isNull( description ) )
        {
            script.addTab(  3  ).addLine( "AND description IS NULL" );
        }
        else
        {
            script.addTab(  3  ).addLine( "AND description = ?");
            script.addArgument( description );
        }

        if ( Objects.isNull( srid ) )
        {
            script.addTab(  3  ).addLine( "AND srid IS NULL" );
        }
        else
        {
            script.addTab(  3  ).addLine( "AND srid = ? ");
            script.addArgument( srid );
        }

        if ( Objects.isNull( wkt ) )
        {
            script.addTab(  3  ).addLine( "AND wkt IS NULL" );
        }
        else
        {
            script.addTab(  3  ).addLine( "AND wkt = ? ");
            script.addArgument( wkt );
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

        LOGGER.trace( "Did I create Feature ID {}? {}",
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
        FeatureDetails that = ( FeatureDetails ) o;
        return id == that.id &&
               Objects.equals( key, that.key );
    }


    @Override
    public int hashCode()
    {
        return Objects.hash( id, key );
    }

}

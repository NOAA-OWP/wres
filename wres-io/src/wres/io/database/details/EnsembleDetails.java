package wres.io.database.details;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.DataProvider;
import wres.io.database.DataScripter;
import wres.io.database.Database;

/**
 * Describes basic information used to define an Ensemble trace from the database
 * @author Christopher Tubbs
 */
public final class EnsembleDetails extends CachedDetail<EnsembleDetails, String>
{
    /**
     * Creates an instance.
     * @param name the name
     */
    public EnsembleDetails( String name )
    {
        this.ensembleName = name;
    }

    /**
     * Creates an instance.
     */
    public EnsembleDetails()
    {
        super();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleDetails.class );

    /**
     The name of the ensemble trace being represented
     */
    private String ensembleName = null;

    /**
     * The serial id of the ensemble in the database
     */
    private Long ensembleId = null;

    /**
     * Updates the ensemble name if necessary. If the update occurs, the serial id is reset
     * @param ensembleName The new name for the ensemble
     */
    public void setEnsembleName( String ensembleName )
    {
        if ( this.ensembleName == null || !this.ensembleName.equalsIgnoreCase( ensembleName ) )
        {
            this.ensembleName = ensembleName;
        }
    }

    @Override
    public int compareTo( EnsembleDetails other )
    {
        return this.getKey().compareTo( other.getKey() );
    }

    @Override
    public String getKey()
    {
        return this.ensembleName;
    }

    @Override
    public Long getId()
    {
        return this.ensembleId;
    }

    @Override
    protected String getIDName()
    {
        return "ensemble_id";
    }

    @Override
    public void setID( long id )
    {
        this.ensembleId = id;
    }

    @Override
    protected DataScripter getInsertSelect( Database database )
    {
        DataScripter script = new DataScripter( database );

        script.setUseTransaction( true );

        script.retryOnSerializationFailure();
        script.retryOnUniqueViolation();

        script.setHighPriority( true );

        script.addTab().addLine( "INSERT INTO wres.Ensemble( ensemble_name ) " );
        script.addTab().addLine( "SELECT ?" );
        script.addArgument( this.ensembleName );
        script.addTab().addLine( "WHERE NOT EXISTS (" );
        script.addTab( 2 ).addLine( "SELECT 1" );
        script.addTab( 2 ).addLine( "FROM wres.Ensemble" );
        script.addTab( 2 ).addLine( "WHERE ensemble_name = ?" );
        script.addArgument( this.ensembleName );
        script.addTab().addLine( ")" );

        return script;
    }

    @Override
    public void save( Database database ) throws SQLException
    {
        DataScripter script = this.getInsertSelect( database );
        boolean performedInsert = script.execute() > 0;

        if ( performedInsert )
        {
            this.ensembleId = script.getInsertedIds()
                                    .get( 0 );
        }
        else
        {
            DataScripter scriptWithId = new DataScripter( database );
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.addLine( "SELECT ensemble_id" );
            scriptWithId.addLine( "FROM wres.Ensemble" );
            scriptWithId.addLine( "WHERE ensemble_name = ?" );
            scriptWithId.addArgument( this.ensembleName );
            scriptWithId.setMaxRows( 1 );

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.ensembleId = data.getLong( this.getIDName() );
            }
        }

        LOGGER.trace( "Did I create Ensemble ID {}? {}",
                      this.ensembleId,
                      performedInsert );
    }

    @Override
    protected Object getSaveLock()
    {
        return new Object();
    }

    @Override
    protected Logger getLogger()
    {
        return EnsembleDetails.LOGGER;
    }

    @Override
    public String toString()
    {
        return "Ensemble: " + this.ensembleName;
    }
}

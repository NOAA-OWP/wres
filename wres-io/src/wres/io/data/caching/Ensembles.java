package wres.io.data.caching;

import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.EnsembleCondition;
import wres.io.data.details.EnsembleDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Cached details about Ensembles from the database
 * @author Christopher Tubbs
 */
public class Ensembles extends Cache<EnsembleDetails, String> {

    private static final int MAX_DETAILS = 500;

    private static final Logger LOGGER = LoggerFactory.getLogger(Ensembles.class);

    private final Database database;
	private final Object detailLock = new Object();
	private final Object keyLock = new Object();

    public Ensembles( Database database )
    {
        this.database = database;
        this.initialize();
    }

    @Override
    protected Database getDatabase()
    {
        return this.database;
    }

	@Override
	protected Object getDetailLock()
	{
		return this.detailLock;
	}

	@Override
	protected Object getKeyLock()
	{
		return this.keyLock;
	}

	private void populate(DataProvider data)
    {
        EnsembleDetails detail;

        while (data.next()) {
            detail = new EnsembleDetails();
            detail.setEnsembleName(data.getString("ensemble_name"));
            detail.setID(data.getInt("ensemble_id"));
            this.add( detail );
        }
    }


    /**
     * Creates a list containing all ensemble IDs that are specified in the condition
     * <p>
     *     The exclude flag is not respected. It is up to the caller to dictate
     *     that the returned values should be excluded
     * </p>
     * @param ensemble An ensemble condition from the project configuration
     * @return All ensemble Ids that match the ensemble conditions
     */
	public List<Long> getEnsembleIDs( EnsembleCondition ensemble )
	{
	    return this.getKeyIndex()
                   .entrySet()
                   .stream()
                   .filter( kv -> kv.getKey()
                                    .equals( ensemble.getName() ) )
                   .map( Entry::getValue )
                   .collect( Collectors.toList() );
	}

	/**
	 * Returns the ID of an Ensemble trace from the global cache based on its name
	 * @param name The name of the Ensemble trace to retrieve
	 * @return The surrogate key, database ID of the Ensemble
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	public Long getEnsembleID( String name ) throws SQLException
    {
        // If there are no identifiers...
        if ( Objects.isNull( name ) )
        {
            // return the default ID
            return this.getDefaultEnsembleID();
        }

		return this.getID( new EnsembleDetails( name ) );
	}

	/**
     * Returns the name of an ensemble trace from the global cache based on its identifier
     * @param ensembleId The ensemble identifier
     * @return The name
     * @throws SQLException Thrown if the ID could not be retrieved from the database
     */
    public String getEnsembleName( long ensembleId ) throws SQLException
    {
        EnsembleDetails details = super.get( ensembleId );       
        return details.getKey();
    }
	
	public Long getDefaultEnsembleID() throws SQLException
    {
        return this.getEnsembleID( "default" );
    }

	@Override
	protected int getMaxDetails() {
		return Ensembles.MAX_DETAILS;
	}
	
	/**
	 * Loads all pre-existing Ensembles into the instanced cache
	 */
    private synchronized void initialize()
	{
        try
        {
            this.initializeDetails();

            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.setHighPriority( true );

            script.addLine( "SELECT ensemble_id, ensemble_name" );
            script.addLine( "FROM wres.Ensemble" );
            script.setMaxRows( MAX_DETAILS );

            try (DataProvider data = script.getData())
            {
                this.populate( data );
            }

            LOGGER.debug( "Finished populating the Ensembles details." );
        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Ensemble cache.", error);
        }
	}
}

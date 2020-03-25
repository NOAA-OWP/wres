package wres.io.data.caching;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiPredicate;

import com.google.common.collect.TreeMultiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.EnsembleCondition;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.NetCDF;
import wres.util.Strings;

/**
 * Cached details about Ensembles from the database
 * @author Christopher Tubbs
 */
public class Ensembles extends Cache<EnsembleDetails, EnsembleKey> {

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

    public Collection<EnsembleDetails> getEnsembleDetails(final Collection<Integer> ensembleIDs)
    {
        TreeMultiset<EnsembleDetails> ensembleDetails = TreeMultiset.create();

        for (Integer id : ensembleIDs)
        {
            if ( this.getDetails()
                     .containsKey( id ))
            {
                ensembleDetails.add( this.get( id ));
            }
        }

        return ensembleDetails;
    }

	private void populate(DataProvider data)
    {
        EnsembleDetails detail;

        while (data.next()) {
            detail = new EnsembleDetails();
            detail.setEnsembleName(data.getString("ensemble_name"));
            detail.setEnsembleMemberIndex( data.getInt( "ensemblemember_id"));
            detail.setQualifierID(data.getString("qualifier_id"));
            detail.setID(data.getInt("ensemble_id"));
            this.add( detail );
        }
    }

	/**
	 * Returns the ensemble ID.
	 * @param ensemble an ensemble
	 * @return an ensemble identifier
	 * @throws SQLException if the ID could not be retrieved from the database
	 */
	
	public Integer getEnsembleID( NetCDF.Ensemble ensemble )
            throws SQLException
    {
		return this.getEnsembleID( ensemble.getName(),
                                   ensemble.getMember(),
                                   ensemble.getQualifier() );
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
	public List<Integer> getEnsembleIDs( EnsembleCondition ensemble )
	{
	    List<Integer> ids = new ArrayList<>();

        BiPredicate<EnsembleCondition, EnsembleKey> namesAreEquivalent = (condition, key) -> {
            boolean compareNames = condition.getName() != null;

            return !compareNames || condition.getName().equals( key.getEnsembleName() );
        };

        BiPredicate<EnsembleCondition, EnsembleKey> membersAreEquivalent = (condition, key) -> {
            boolean compareMembers = condition.getMemberId() != null;
            return !compareMembers || Integer.parseInt(condition.getMemberId()) == key.getMemberIndex();
        };

        BiPredicate<EnsembleCondition, EnsembleKey> qualifiersAreEquivalent = (condition, key) -> {
            boolean compareQualifiers = condition.getQualifier() != null;
            return !compareQualifiers || condition.getQualifier().equals( key.getQualifierID() );
        };

	    for (Entry<EnsembleKey, Integer> key : this.getKeyIndex().entrySet())
        {
            EnsembleKey ensembleKey = key.getKey();

            if (namesAreEquivalent.and( membersAreEquivalent ).and( qualifiersAreEquivalent ).test( ensemble, ensembleKey ))
            {
                ids.add(key.getValue());
            }
        }
		return ids;
	}

    /**
     * Gets the id of a single ensemble attached to the project
     * <p>
     *     Being able to query by a single ensemble speeds up queries where
     *     information about each query is not neccessary
     * </p>
     * @param projectId The id of the project to check
     * @param variableFeatureId The id of the variable and geospatial position
     * @return An id of an ensemble for the project
     * @throws SQLException Thrown if an ensemble could not be retrieved
     */
	public Integer getSingleEnsembleID( Integer projectId, Integer variableFeatureId )
            throws SQLException
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.setHighPriority( true );

        script.addLine("SELECT E.ensemble_id");
        script.addLine("FROM wres.Ensemble E");
        script.addLine("WHERE EXISTS (");
        script.addTab().addLine("SELECT 1");
        script.addTab().addLine("FROM wres.TimeSeries TS");
        script.addTab().addLine("WHERE TS.variablefeature_id = ", variableFeatureId);
        script.addTab(  2  ).addLine("AND TS.ensemble_id = E.ensemble_id");
        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", projectId);
        script.addTab(    4    ).addLine("AND PS.member = 'right'");
        script.addTab(    4    ).addLine("AND TS.source_id = PS.source_id");
        script.addTab(  2  ).addLine(")");
        script.addLine(")");
        script.addLine("LIMIT 1;");

        return script.retrieve( "ensemble_id" );
    }
	
	/**
	 * Returns the ID of an Ensemble from the global cache based on the combination of its name, member ID, and qualifier
	 * @param name The name of the Ensemble to retrieve
	 * @param memberIndex The Member Index of the Ensemble to retrieve
	 * @param qualifierID The qualifier of the Ensemble to retrieve
	 * @return The ID of the Ensemble
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	public Integer getEnsembleID(String name, Integer memberIndex, String qualifierID) throws SQLException
    {
        // If there is no name, but there are either a member ID or a qualifier...
	    if (name == null && ( memberIndex != null || Strings.hasValue( qualifierID )))
        {
            // just set the name as blank
            name = "";
        }
        // If there are no identifiers...
        else if (name == null)
        {
            // return the default ID
            return this.getDefaultEnsembleID();
        }

		return this.getID( new EnsembleDetails( name, memberIndex, qualifierID ) );
	}

	public Integer getDefaultEnsembleID() throws SQLException
    {
        return this.getEnsembleID( "default", null, null );
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

            script.addLine("SELECT ensemble_id, ensemble_name, qualifier_id, ensemblemember_id");
            script.addLine("FROM wres.Ensemble");
            script.addLine("LIMIT ", MAX_DETAILS, ";");

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

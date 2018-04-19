package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.EnsembleCondition;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.NetCDF;

/**
 * Cached details about Ensembles from the database
 * @author Christopher Tubbs
 */
public class Ensembles extends Cache<EnsembleDetails, EnsembleKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ensembles.class);
    private static final Object CACHE_LOCK = new Object();

	private static final Object DETAIL_LOCK = new Object();
	private static final Object KEY_LOCK = new Object();

	@Override
	protected Object getDetailLock()
	{
		return Ensembles.DETAIL_LOCK;
	}

	@Override
	protected Object getKeyLock()
	{
		return Ensembles.KEY_LOCK;
	}

    /**
     *  Internal cache that will store a global collection of details whose details may be accessed through static methods
     */
	private static Ensembles instance = null;

	private static Ensembles getCache()
	{
		synchronized (CACHE_LOCK)
		{
			if ( instance == null)
			{
				instance = new Ensembles();
				instance.init();
			}
			return instance;
		}
	}

	/**
	 * Returns the ensemble ID.
	 * @param ensemble an ensemble
	 * @return an ensemble identifier
	 * @throws SQLException if the ID could not be retrieved from the database
	 */
	
	public static Integer getEnsembleID( NetCDF.Ensemble ensemble )
            throws SQLException
    {
		return Ensembles.getEnsembleID( ensemble.getName(),
                                           String.valueOf( ensemble.getTMinus()),
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
     * @throws SQLException if the ensemble IDs could not be obtained
     */
	public static List<Integer> getEnsembleIDs( EnsembleCondition ensemble)
			throws SQLException
	{
	    List<Integer> ids = new ArrayList<>();

        BiPredicate<EnsembleCondition, EnsembleKey> namesAreEquivalent = (condition, key) -> {
            boolean compareNames = ensemble.getName() != null;

            return !compareNames || condition.getName().equals( key.getEnsembleName() );
        };

        BiPredicate<EnsembleCondition, EnsembleKey> membersAreEquivalent = (condition, key) -> {
            boolean compareMembers = ensemble.getMemberId() != null;
            return !compareMembers || condition.getMemberId().equals( key.getMemberIndex() );
        };

        BiPredicate<EnsembleCondition, EnsembleKey> qualifiersAreEquivalent = (condition, key) -> {
            boolean compareQualifiers = condition.getQualifier() != null;
            return !compareQualifiers || condition.getQualifier().equals( key.getQualifierID() );
        };

	    for (Map.Entry<EnsembleKey, Integer> key : Ensembles.getCache().getKeyIndex().entrySet())
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
     * @param variablePositionId The id of the variable and geospatial position
     * @return An id of an ensemble for the project
     * @throws SQLException Thrown if an ensemble could not be retrieved
     */
	public static Integer getSingleEnsembleID(Integer projectId, Integer variablePositionId)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("SELECT E.ensemble_id");
        script.addLine("FROM wres.Ensemble E");
        script.addLine("WHERE EXISTS (");
        script.addTab().addLine("SELECT 1");
        script.addTab().addLine("FROM wres.TimeSeries TS");
        script.addTab().addLine("WHERE TS.variableposition_id = ", variablePositionId);
        script.addTab(  2  ).addLine("AND TS.ensemble_id = E.ensemble_id");
        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        script.addTab(   3   ).addLine("INNER JOIN wres.ForecastSource FS");
        script.addTab(    4    ).addLine("ON PS.source_id = FS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", projectId);
        script.addTab(    4    ).addLine("AND PS.member = 'right'");
        script.addTab(    4    ).addLine("AND FS.forecast_id = TS.timeseries_id");
        script.addTab(  2  ).addLine(")");
        script.addLine(")");
        script.addLine("LIMIT 1;");

        return script.retrieve( "ensemble_id" );
    }
	
	/**
	 * Returns the ID of an Ensemble from the global cache based on the combination of its name, member ID, and qualifier
	 * @param name The name of the Ensemble to retrieve
	 * @param memberID The Member ID of the Ensemble to retrieve
	 * @param qualifierID The qualifier of the Ensemble to retrieve
	 * @return The ID of the Ensemble
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getEnsembleID(String name, String memberID, String qualifierID) throws SQLException {
		return Ensembles.getCache().getID( new EnsembleDetails( name, memberID, qualifierID ) );
	}
	
	@Override
	protected int getMaxDetails() {
		return 500;
	}
	
	/**
	 * Loads all pre-existing Ensembles into the instanced cache
	 */
	@Override
    protected synchronized void init()
	{
        Connection connection = null;
        Statement ensembleQuery = null;
        ResultSet ensembles = null;
        try
        {
            connection = Database.getHighPriorityConnection();
            ensembleQuery = connection.createStatement();
            
            String loadScript = "SELECT ensemble_id, ensemble_name, qualifier_id, ensemblemember_id" + NEWLINE;
            loadScript += "FROM wres.ensemble" + NEWLINE;
            loadScript += "LIMIT " + getMaxDetails();
            
            ensembles = ensembleQuery.executeQuery(loadScript);
            
            EnsembleDetails detail;
            
            while (ensembles.next()) {
                detail = new EnsembleDetails();
                detail.setEnsembleName(ensembles.getString("ensemble_name"));
                detail.setEnsembleMemberID(String.valueOf(ensembles.getInt("ensemblemember_id")));
                detail.setQualifierID(ensembles.getString("qualifier_id"));
                detail.setID(ensembles.getInt("ensemble_id"));
                
                this.add(detail.getKey(), detail.getId());
            }
        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Ensemble cache.", error);
        }
        finally
        {
            if (ensembles != null)
            {
                try
                {
                    ensembles.close();
                }
                catch(SQLException e)
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "An error was encountered when trying to close the result set containing ensemble information.", e);
                }
            }

            if (ensembleQuery != null)
            {
                try
                {
                    ensembleQuery.close();
                }
                catch(SQLException e)
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "An error was encountered when trying to close the query that loaded ensemble information.", e );
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
	}
}

package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.EnsembleCondition;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.io.utilities.Database;
import wres.util.Collections;
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

	public static Integer getEnsembleID( NetCDF.Ensemble ensemble )
            throws SQLException
    {
		return Ensembles.getEnsembleID( ensemble.getName(),
                                           String.valueOf( ensemble.getTMinus()),
                                           ensemble.getQualifier() );
	}

	public static Integer getEnsembleID( EnsembleCondition ensemble)
			throws SQLException
	{
		return Ensembles.getCache().getID(
				new EnsembleDetails(ensemble.getName(),
									ensemble.getMemberId(),
									ensemble.getQualifier() )
		);
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

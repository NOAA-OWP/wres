package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.io.data.details.VariableDetails;
import wres.io.utilities.Database;

/**
 * @author Christopher Tubbs
 *
 * Manages the retrieval of variable information that may be shared across threads
 */
public final class Variables extends Cache<VariableDetails, String>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(Variables.class);
    /**
     * The global cache of variables whose details may be accessed through static methods
     */
	private static Variables instance = null;
	private static final Object CACHE_LOCK = new Object();

	private static Variables getCache()
	{
		synchronized (CACHE_LOCK)
		{
			if ( instance == null)
			{
				instance = new Variables();
				instance.init();
			}
			return instance;
		}
	}
	
	/**
	 * Returns the ID of a variable from the global cache
	 * @param variableName The short name of the variable
	 * @param measurementUnit The name of the unit of measurement for the variable
	 * @return The ID of the variable
	 * @throws SQLException if the ID could not be retrieved
	 */
	public static Integer getVariableID(String variableName, String measurementUnit) throws SQLException {
		return getCache().getID(variableName, measurementUnit);
	}
	
	/**
	 * Returns the ID of a variable from the global cache
	 * @param variableName The short name of the variable
	 * @param measurementUnitID The ID of the unit of measurement belonging to the variable
	 * @return The ID of the variable
	 * @throws SQLException Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public static Integer getVariableID(String variableName, Integer measurementUnitID) throws SQLException {
		return getCache().getID(variableName, measurementUnitID);
	}

	public static Integer getVariableID( DataSourceConfig dataSourceConfig) throws SQLException
	{
		return Variables.getVariableID(dataSourceConfig.getVariable().getValue(),
									   dataSourceConfig.getVariable().getUnit());
	}
	
	/**
	 * Returns the ID of the variable from the instance cache
	 * @param variableName The short name of the variable
	 * @param measurementUnit The name of the unit of measurement for the variable
	 * @return The ID of the variable
     * @throws SQLException if the ID could not be added to the cache
	 */
	public Integer getID(String variableName, String measurementUnit) throws SQLException {
		if (!getKeyIndex().containsKey(variableName)) {
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			detail.setMeasurementunitId( MeasurementUnits.getMeasurementUnitID(measurementUnit) );
            try
			{
                addElement(detail);
            }
            catch (SQLException e) {
                String message = "The variable '" + variableName + "' could not be added to the cache.";
                LOGGER.error(message);
                throw new SQLException(message, e);
            }
        }

		return this.getKeyIndex().get(variableName);
	}
	
	/**
	 * Returns the ID of the variable from the instance cache
	 * @param variableName The short name of the variable
	 * @param measurementUnitID The ID of the unit of measurement for the variable
	 * @return The ID of the variable
	 * @throws SQLException Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public Integer getID(String variableName, Integer measurementUnitID) throws SQLException
	{
		if (!getKeyIndex().containsKey(variableName)) {
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			detail.setMeasurementunitId( measurementUnitID );
			addElement(detail);
		}
		return this.getKeyIndex().get(variableName);
	}

	public static String getName(Integer variableId)
	{
		return getCache().getKey(variableId);
	}

	public String getKey(Integer variableId)
	{
		String name = null;

		if (this.get(variableId) != null)
		{
			name = this.get(variableId).getKey();
		}

		return name;
	}

	public static VariableDetails getByName(String name) throws SQLException
	{
		VariableDetails details = null;

		if (Variables.getCache().hasID( name ))
		{
			Integer id = Variables.getCache().getID( name );
			details = Variables.getCache().get( id );
		}

		return details;
	}

	@Override
	protected int getMaxDetails() {
		return 100;
	}

    @Override
    protected void init()
    {       
        synchronized(this.getKeyIndex())
        {
            Connection connection = null;
            ResultSet variables = null;

            this.initializeDetails();

            try
            {
                connection = Database.getHighPriorityConnection();

                String loadScript = "SELECT variable_id, variable_name, measurementunit_id" + System.lineSeparator();
                loadScript += "FROM wres.variable;";

                variables = Database.getResults(connection, loadScript);
                VariableDetails detail;

                while (variables.next())
				{
                    detail = VariableDetails.from(variables);
                    this.getKeyIndex().put(detail.getKey(), detail.getId());
                    this.getDetails().put(detail.getId(), detail);
                }
            }
            catch ( SQLException sqlException )
            {
				// Failure to pre-populate cache should not affect primary outputs.
                LOGGER.warn( "An error was encountered when trying to populate "
                             + "the Variable cache.", sqlException );
            }
            finally
            {
                if (variables != null)
                {
                    try
                    {
                        variables.close();
                    }
                    catch(SQLException e)
                    {
						// Exception on close should not affect primary outputs.
                        LOGGER.warn( "An error was encountered when trying to"
                                     + "close the result set containing Variable "
                                     + "information.", e );
                    }
                }

                if (connection != null)
                {
                    Database.returnHighPriorityConnection(connection);
                }
            }
        }
    }
}

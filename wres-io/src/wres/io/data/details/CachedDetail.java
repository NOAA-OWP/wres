package wres.io.data.details;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import wres.io.utilities.Database;

/**
 * Describes detail about data from the database that may be accessed through a global cache
 * @author Christopher Tubbs
 */
public abstract class CachedDetail<U, V extends Comparable<V>> implements Comparable<U> {
    /**
     * System independent definition for a newline string
     */
	static final String NEWLINE = System.lineSeparator();
	
	/**
	 * @return The key for the value used to access the value
	 */
	public abstract V getKey();
	
	/**
	 * @return The primary ID of the detail definition
	 */
	public abstract Integer getId();
	
	/**
	 * @return The name of the column that stores the ID in the database
	 */
	protected abstract String getIDName();
	
	/**
	 * Sets the ID of the detail as defined in the database
	 * @param id The ID for the detail in the database
	 */
	protected abstract void setID( Integer id );
	
	/**
	 * @return A script used to select the ID of the detail in the data base and insert it if need be
	 * @throws SQLException if the insert select statement failed
	 */
	protected abstract String getInsertSelectStatement() throws SQLException;

	protected abstract Object getSaveLock();

	/**
	 * Updates the detail with information loaded from the database
	 * @param databaseResults Information retrieved from the database
	 * @throws SQLException Thrown if the requested values couldn't be retrieved from the resultset
	 */
	protected void update(ResultSet databaseResults) throws SQLException
	{
		if (Database.hasColumn( databaseResults, this.getIDName() ))
		{
			this.setID( Database.getValue( databaseResults, this.getIDName() ) );
		}
	}
	
	/**
	 * Saves the ID of the detail from the database based on the result of the of the insert/select statement
	 * @throws SQLException if the save failed
	 */
	public void save() throws SQLException
	{
		synchronized ( this.getSaveLock() )
		{
			Connection connection = null;
			ResultSet results = null;

			try
			{
				connection = Database.getConnection();
				results = Database.getResults( connection, this.getInsertSelectStatement() );
				this.update( results );
			}
			finally
			{
				if ( results != null)
				{
					results.close();
				}

				if ( connection != null )
				{
					Database.returnConnection( connection );
				}
			}
		}
	}
}

package wres.io.data.details;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;

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
     * @param connection The connection that the statement will belong to
     * @return A statement that can be used to safely execute an insert and select query
     * @throws SQLException Thrown if the connection and query cannot be used
     * to create the statement
     */
	protected abstract PreparedStatement getInsertSelectStatement( Connection connection) throws SQLException;

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
		else
        {
            throw new SQLException( "No data could be loaded for " + this );
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
            PreparedStatement statement = null;
			try
			{
				connection = Database.getConnection();
				statement = this.getInsertSelectStatement( connection );
				results = statement.executeQuery();

                if (!results.isBeforeFirst())
                {
                    throw new SQLException( "No value can be loaded with the key of: " +
                                            this.getKey() );
                }

				this.update( results );
			}
			finally
			{
				if ( results != null)
				{
				    try
                    {
                        results.close();
                    }
                    catch( SQLException e )
                    {
                        // Failure to close should not affect primary outputs
                        this.getLogger().warn("Failed to close result set {}.", results, e);
                    }
				}

                if ( statement != null)
                {
                    try
                    {
                        statement.close();
                    }
                    catch (SQLException e)
                    {
                        // Failure to close should not affect primary outputs
                        this.getLogger().warn( "Failed to close statement {}.",
                                               statement,
                                               e );
                    }
                }

				if ( connection != null )
				{
					Database.returnConnection( connection );
				}
			}
		}
	}

	protected abstract Logger getLogger();

	@Override
	public abstract String toString();
}

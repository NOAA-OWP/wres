package wres.io.data.details;

import java.sql.SQLException;

import org.slf4j.Logger;

import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Describes detail about data from the database that may be accessed through a global cache
 * @author Christopher Tubbs
 */
public abstract class CachedDetail<U, V extends Comparable<V>> implements Comparable<U> {
	
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
	 * @param database The database to use.
     * @return A statement that can be used to safely execute an insert and select query
     * @throws SQLException Thrown if the connection and query cannot be used
     * to create the statement
     */
	protected abstract DataScripter getInsertSelect( Database database ) throws SQLException;

	protected abstract Object getSaveLock();

	/**
	 * Updates the detail with information loaded from the database
	 * @param databaseResults Information retrieved from the database
	 * @throws SQLException Thrown if the requested values couldn't be retrieved from the resultset
	 */
	protected void update(DataProvider databaseResults) throws SQLException
	{
		if (databaseResults.hasColumn( this.getIDName() ))
		{
			this.setID( databaseResults.getValue( this.getIDName() ));
		}
		else
        {
            throw new SQLException( "No data could be loaded for " + this );
        }
	}

	public CachedDetail(DataProvider resultSet) throws SQLException
	{
		this.update( resultSet );
	}

	CachedDetail(){}

    /**
	 * @param database The database to use.
	 * Saves the ID of the detail from the database based on the result of the of the insert/select statement
	 * @throws SQLException if the save failed
	 */
	public void save( Database database ) throws SQLException
	{
		synchronized ( this.getSaveLock() )
		{
			try ( DataProvider results = this.getInsertSelect( database )
											 .getData() )
			{
                if (results.isEmpty())
                {
                    throw new SQLException( "No value can be loaded with the key of: " +
                                            this.getKey() );
                }

				this.update( results );
			}
			catch(SQLException e)
            {
                this.getLogger().error( "Failed to save: ", this );
                throw e;
            }
		}
	}

	protected abstract Logger getLogger();

	@Override
	public abstract String toString();
}

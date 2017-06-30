package wres.io.data.details;

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
	public abstract void setID(Integer id);
	
	/**
	 * @return A script used to select the ID of the detail in the data base and insert it if need be
	 */
	protected abstract String getInsertSelectStatement();
	
	/**
	 * Saves the ID of the detail from the database based on the result of the of the insert/select statement
	 * @throws SQLException
	 */
	public void save() throws SQLException
	{
		setID(Database.getResult(getInsertSelectStatement(), getIDName()));
	}
}

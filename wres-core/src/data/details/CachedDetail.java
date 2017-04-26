/**
 * 
 */
package data.details;

import java.sql.SQLException;

import util.Database;

/**
 * @author ctubbs
 *
 */
public abstract class CachedDetail<U, V extends Comparable<V>> implements Comparable<U> {
	static final String newline = System.lineSeparator();
	
	public abstract V getKey();
	public abstract Integer getId();
	protected abstract String getIDName();
	public abstract void setID(Integer id);
	protected abstract String getInsertSelectStatement();
	
	public void save() throws SQLException
	{
		setID(Database.get_result(getInsertSelectStatement(), getIDName()));
	}
}

/**
 * 
 */
package config.data;

import java.util.TreeMap;

import javax.xml.stream.XMLStreamReader;

/**
 * A ConfigElement that can generate a clause for a SQL statement based on passed in table names 
 * and their Aliases
 * @author Christopher Tubbs
 */
public abstract class ClauseConfig extends ConfigElement{
	public ClauseConfig(XMLStreamReader reader) {
		super(reader);
	}

	/**
	 * Uses the passed in aliases to generate statements to be added to a where clause in a SQL statement
	 * @param aliases A mapping between tables used in the SQL statement and their aliases
	 * @return A string containing elements that may be added to a where clause
	 */
	public abstract String getCondition(TreeMap<String, String> aliases);
}

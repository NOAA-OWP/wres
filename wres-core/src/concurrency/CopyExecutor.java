/**
 * 
 */
package concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.Database;

/**
 * @author ctubbs
 *
 */
public class CopyExecutor implements Runnable
{
    private final String table_definition;
    private final String values;
    private final String delimiter;

    private static final Logger LOGGER = LoggerFactory.getLogger(CopyExecutor.class);

	/**
	 * 
	 */
	public CopyExecutor(String table_definition, String values, String delimiter) 
	{
		this.table_definition = table_definition;
		this.values = values;
		this.delimiter = delimiter;
	}

	@Override
	/**
	 * Copy the passed in data to the database
	 */
	public void run() {
		try {
		    LOGGER.trace("Using table_definition {} values {} delimiter {}",
		                 table_definition, values, delimiter);
			Database.copy(table_definition, values, delimiter);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
}

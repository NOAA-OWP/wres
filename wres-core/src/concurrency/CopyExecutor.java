/**
 * 
 */
package concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.Database;
import util.Utilities;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
public class CopyExecutor extends WRESThread implements Runnable
{
    private final String table_definition;
    private final String values;
    private final String delimiter;

    private static final Logger LOGGER = LoggerFactory.getLogger(CopyExecutor.class);

    /**
     * The Constructor
     * @param table_definition The definition of the table and values to insert and in what order
     * @param values Newline delimited string containing values delimited by the delimiter that adheres to the table definition
     * @param delimiter The symbol separating each value in each line of the values
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
	    this.executeOnRun();
		try {
		    LOGGER.trace("Using table_definition {} values {} delimiter {}",
		                 table_definition, values, delimiter);
			Database.copy(table_definition, values, delimiter);
		} catch (Exception e) {
			e.printStackTrace();
		}		
		this.exectureOnComplete();
	}
}

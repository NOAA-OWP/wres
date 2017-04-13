/**
 * 
 */
package concurrency;

import util.Database;

/**
 * @author ctubbs
 *
 */
public class CopyExecutor implements Runnable {

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
			Database.copy(table_definition, values, delimiter);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private final String table_definition;
	private final String values;
	private String delimiter;
}

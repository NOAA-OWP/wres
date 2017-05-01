/**
 * 
 */
package concurrency;

import java.sql.SQLException;

import util.Database;

/**
 * A thread that will execute a passed in SQL script
 * 
 * @author Christopher Tubbs
 */
public class SQLExecutor implements Runnable {

	/**
	 * Creates the thread with the passed in SQL script
	 * @param script The script to execute
	 */
	public SQLExecutor(String script) {
		this.script = script;
	}

	@Override
	/**
	 * Executes the SQL script
	 */
	public void run() {
		try {
			Database.execute(this.script);
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}

	private String script = null;
}

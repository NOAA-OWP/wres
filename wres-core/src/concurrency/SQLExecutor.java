/**
 * 
 */
package concurrency;

import java.sql.SQLException;

import util.Database;

/**
 * A thread that will execute a passed in SQL script
 */
public class SQLExecutor implements Runnable {

	/**
	 * Creates the thread with the passed in SQL script
	 * @param script The script to execute
	 */
	public SQLExecutor(String script) {
		this.script = script;
	}
	
	public SQLExecutor(String script, boolean commit)
	{
		this.script = script;
		this.commit = commit;
	}

	@Override
	/**
	 * Executes the SQL script
	 */
	public void run() {
		try {
			Database.execute(this.script/*, commit*/);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	private String script = null;
	private boolean commit = true;
}

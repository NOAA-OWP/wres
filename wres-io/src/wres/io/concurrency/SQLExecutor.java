package wres.io.concurrency;

import java.sql.SQLException;

import wres.io.utilities.Database;

/**
 * A thread that will execute a passed in SQL script
 * 
 * @author Christopher Tubbs
 */
public class SQLExecutor extends WRESThread implements Runnable
{

	/**
	 * Creates the thread with the passed in SQL script
	 * @param script The script to execute
	 */
	public SQLExecutor(String script) {
		this.script = script;
	}

	@Override
    public void run() {
	    this.executeOnRun();
		try {
			Database.execute(this.script);
		} catch (SQLException e) {
		    // Error Information is handled by the database module
		}		
		this.executeOnComplete();
	}

	private String script = null;
}

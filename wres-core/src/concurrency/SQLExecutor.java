/**
 * 
 */
package concurrency;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.Database;

/**
 * A thread that will execute a passed in SQL script
 * 
 * @author Christopher Tubbs
 */
public class SQLExecutor extends WRESThread implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExecutor.class);

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
	    this.executeOnRun();
		try {
			Database.execute(this.script);
		} catch (SQLException e) {
		    LOGGER.debug("Script: {}", script);
			e.printStackTrace();
		}		
		this.exectureOnComplete();
	}

	private String script = null;
}

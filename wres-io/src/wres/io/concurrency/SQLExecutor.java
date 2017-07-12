package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.utilities.Database;

import java.sql.SQLException;

/**
 * A thread that will execute a passed in SQL script
 * 
 * @author Christopher Tubbs
 */
public class SQLExecutor extends WRESTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExecutor.class);

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

	@Override
	protected String getTaskName () {
		return "SQLExecutor: " + String.valueOf(Thread.currentThread().getId());
	}
}

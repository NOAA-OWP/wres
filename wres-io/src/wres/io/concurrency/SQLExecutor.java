package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.utilities.Database;
import wres.util.Internal;

import java.sql.SQLException;

/**
 * A thread that will execute a passed in SQL script
 * 
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class SQLExecutor extends WRESRunnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExecutor.class);

	/**
	 * Creates the thread with the passed in SQL script
	 * @param script The script to execute
	 */
	@Internal(exclusivePackage = "wres.io")
	public SQLExecutor(String script) {
		this.script = script;
	}

	@Override
    public void execute() {
		try {
			Database.execute(this.script);
		} catch (SQLException e) {
		    // Error Information is handled by the database module
		}
	}

	private String script = null;

	@Override
	protected String getTaskName () {
		return "SQLExecutor";
	}

	@Override
	protected Logger getLogger () {
		return SQLExecutor.LOGGER;
	}
}

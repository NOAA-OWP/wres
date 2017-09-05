package wres.io.concurrency;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

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

	/**
	 * Creates the thread with the passed in SQL script with the option to
	 * display log errors in the database or to log them without
	 * necessarily displaying them
	 * @param script The script to execute
	 * @param displayErrors Determines if errors occuring in the database
	 *                      should be logged/displayed as errors or logged as
	 *                      debug records
	 */
	@Internal(exclusivePackage = "wres.io")
	public SQLExecutor(String script, boolean displayErrors)
	{
		this.script = script;
		this.displayErrors = displayErrors;
	}

	@Override
    public void execute() {
		try
		{
			Database.execute(this.script);
		} catch (SQLException e) {
			// We allow the hiding of errors on the screen on the rare
			// occasion where failing in the database is acceptable
			if (this.displayErrors)
			{
				this.getLogger().error( Strings.getStackTrace( e ) );
			}
			else
			{
				this.getLogger().debug( Strings.getStackTrace( e ) );
			}
		}
	}

	private String script = null;
	private boolean displayErrors = true;

	@Override
	protected Logger getLogger () {
		return SQLExecutor.LOGGER;
	}
}

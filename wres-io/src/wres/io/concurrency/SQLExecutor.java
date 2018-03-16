package wres.io.concurrency;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Database;
import wres.util.Strings;

/**
 * A thread that will execute a passed in SQL script
 * 
 * @author Christopher Tubbs
 */
public class SQLExecutor extends WRESRunnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExecutor.class);

	/**
	 * Creates the thread with the passed in SQL script
	 * @param script The script to execute
	 */
	public SQLExecutor(String script) {
		this.script = script;
		this.displayErrors = true;
		this.forceTransaction = false;
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
	public SQLExecutor(String script, boolean displayErrors)
	{
		this.script = script;
		this.displayErrors = displayErrors;
		this.forceTransaction = false;
	}

	public SQLExecutor(String script, boolean displayErrors, boolean forceTransaction)
    {
        this.script = script;
        this.displayErrors = displayErrors;
        this.forceTransaction = forceTransaction;
    }

	@Override
    public void execute() throws SQLException
    {
		try
		{
            Database.execute( this.script, forceTransaction );
		}
		catch (SQLException e)
		{
			// We allow the hiding of errors on the screen on the rare
			// occasion where failing in the database is acceptable
			if (this.displayErrors)
			{
				this.getLogger().error( Strings.getStackTrace( e ) );
				throw e;
			}
			else
			{
				this.getLogger().debug( Strings.getStackTrace( e ) );
			}
		}
	}

	private final String script;
	private final boolean displayErrors;
	private final boolean forceTransaction;

	@Override
	protected Logger getLogger () {
		return SQLExecutor.LOGGER;
	}
}

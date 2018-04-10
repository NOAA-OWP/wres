package wres.io.concurrency;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Database;

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
		this.forceTransaction = false;
	}

    public SQLExecutor( String script, boolean forceTransaction )
    {
        this.script = script;
        this.forceTransaction = forceTransaction;
    }

	@Override
    public void execute() throws SQLException
    {
        Database.execute( this.script, forceTransaction );
	}

	private final String script;
	private final boolean forceTransaction;

	@Override
	protected Logger getLogger () {
		return SQLExecutor.LOGGER;
	}
}

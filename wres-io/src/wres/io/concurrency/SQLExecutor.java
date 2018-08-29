package wres.io.concurrency;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
    // TODO: Add support for high priority connections
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
        if (this.arguments.isEmpty())
        {
            Database.execute( this.script, forceTransaction );
        }
        else
        {
            Database.execute( this.script, this.arguments );
        }
	}

    public void addBatchArguments( Collection<Object[]> arguments)
    {
        this.arguments.addAll( arguments );
    }

	private final String script;
	private final boolean forceTransaction;
	private final List<Object[]> arguments = new ArrayList<>(  );

	@Override
	protected Logger getLogger () {
		return SQLExecutor.LOGGER;
	}
}

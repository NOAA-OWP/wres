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
public class ValueRetriever<V> extends WRESCallable<V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ValueRetriever.class);

	/**
	 * Creates the thread with the passed in SQL script
	 * @param script The script to execute
	 * @param label The label
	 */
	public ValueRetriever( String script, String label) {
		this.script = script;
		this.label = label;
	}

	@Override
    public V execute() throws SQLException
	{
        return Database.getResult( this.script, this.label );
	}

	private String script = null;
	private String label = null;

	@Override
	protected Logger getLogger () {
		return ValueRetriever.LOGGER;
	}
}

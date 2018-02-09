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

	/**
	 * Creates the thread with the passed in SQL script with the option to
	 * display log errors in the database or to log them without
	 * necessarily displaying them
	 * @param script The script to execute
	 * @param displayErrors Determines if errors occuring in the database
	 *                      should be logged/displayed as errors or logged as
	 *                      debug records
	 * @param label The label
	 */
	public ValueRetriever( String script, String label, boolean displayErrors)
	{
		this.script = script;
		this.displayErrors = displayErrors;
		this.label = label;
	}

	@Override
    public V execute() throws SQLException
	{
		V value = null;
		try
		{
			value = Database.getResult( this.script, this.label );
		}
		catch (SQLException e)
		{
			// We allow the hiding of errors on the screen on the rare
			// occasion where failing in the database is acceptable
			if (this.displayErrors)
			{
				throw e;
			}
			else
			{
				this.getLogger().debug( Strings.getStackTrace( e ) );
			}
		}

		if (value == null && LOGGER.isDebugEnabled())
		{
			LOGGER.debug( "A value named '{}' could not be retrieved for the script:{}{}",
						  label,
						  System.lineSeparator(),
						  script );
		}

		return value;
	}

	private String script = null;
	private boolean displayErrors = true;
	private String label = null;

	@Override
	protected Logger getLogger () {
		return ValueRetriever.LOGGER;
	}
}

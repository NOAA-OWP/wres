package wres.io.concurrency;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.DataSet;
import wres.io.utilities.Database;

/**
 * A thread that will execute a passed in SQL script
 * 
 * @author Christopher Tubbs
 */
public class DataSetRetriever extends WRESCallable<DataSet>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSetRetriever.class);

	/**
	 * Creates the thread with the passed in SQL script
	 * @param script The script to execute
	 */
	public DataSetRetriever( String script)
	{
		this.script = script;
	}

	@Override
    public DataSet execute() throws SQLException
	{
		return Database.getDataSet( this.script );
	}

	private String script = null;

	@Override
	protected Logger getLogger () {
		return DataSetRetriever.LOGGER;
	}
}

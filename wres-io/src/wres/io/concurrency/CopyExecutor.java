package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class CopyExecutor extends WRESRunnable
{
    private final String table_definition;
    private final String values;
    private final String delimiter;

    private static final Logger LOGGER = LoggerFactory.getLogger(CopyExecutor.class);

    /**
     * The Constructor
     * @param table_definition The definition of the table and values to insert and in what order
     * @param values Newline delimited string containing values delimited by the delimiter that adheres to the table definition
     * @param delimiter The symbol separating each value in each line of the values
     */
	@Internal(exclusivePackage = "wres.io")
	public CopyExecutor(String table_definition, String values, String delimiter) 
	{
		this.table_definition = table_definition;
		this.values = values;
		this.delimiter = delimiter;
	}

	@Override
    public void execute() {
		try {
		    this.getLogger().trace("Using table_definition {} values {} delimiter {}",
		                 table_definition, 
		                 values,
		                 delimiter);
		    
			Database.copy(table_definition, values, delimiter);
		} catch (Exception e) {
			this.getLogger().error(Strings.getStackTrace(e));
		}
	}

	@Override
	protected Logger getLogger () {
		return CopyExecutor.LOGGER;
	}
}

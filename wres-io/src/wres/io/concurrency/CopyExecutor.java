package wres.io.concurrency;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Database;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
public class CopyExecutor extends WRESRunnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CopyExecutor.class );

    private final String tableDefinition;
    private final String values;
    private final String delimiter;


    /**
     * The Constructor
     * @param tableDefinition The definition of the table and values to insert and in what order
     * @param values Newline delimited string containing values delimited by the delimiter that adheres to the table definition
     * @param delimiter The symbol separating each value in each line of the values
     */
    public CopyExecutor( String tableDefinition,
                         String values,
                         String delimiter )
    {
        this.tableDefinition = tableDefinition;
        this.values = values;
        this.delimiter = delimiter;
    }

    @Override
    public void execute() throws SQLException
    {
        LOGGER.trace( "Using tableDefinition {} values {} delimiter {}",
                      tableDefinition,
                      values,
                      delimiter );

        Database.copy( tableDefinition, values, delimiter );
    }

    @Override
    protected Logger getLogger ()
	{
        return CopyExecutor.LOGGER;
    }
}

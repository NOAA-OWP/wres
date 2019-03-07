package wres.io.utilities;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.functional.ExceptionalConsumer;
import wres.util.functional.ExceptionalFunction;

public class DataScripter extends ScriptBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DataScripter.class );
    private boolean isHighPriority = false;
    private final List<Object> arguments = new ArrayList<>(  );
    private final Set<String> lockTables = new HashSet<>();
    private boolean useTransaction;

    public DataScripter()
    {
        super();
    }

    public DataScripter(final String beginning)
    {
        super(beginning);
    }

    /**
     * Sets whether or not the script should use high priority connections
     * @param highPriority Whether or not high priority connections should be used
     */
    public void setHighPriority(boolean highPriority)
    {
        this.isHighPriority = highPriority;
    }

    public void setUseTransaction(boolean useTransaction)
    {
        this.useTransaction = useTransaction;
    }

    public void addArgument(final Object argument)
    {
        this.arguments.add(argument);
    }

    public void addTablesToLock(String... tableNames)
    {
        this.lockTables.addAll( Arrays.asList( tableNames ) );
    }

    public void execute(Collection<Object> parameters) throws SQLException
    {
        this.execute(parameters.toArray());
    }

    /**
     * Executes the built script with the given parameters
     * @param parameters The values to use as parameters to the built script
     * @throws SQLException Thrown if execution of the script fails
     */
    public void execute(Object... parameters) throws SQLException
    {
        Database.execute( this.formQuery().setParameters( parameters ), this.isHighPriority );
    }

    /**
     * Executes the script in batch with the given parameters
     * @param parameters A collection of sets of objects to use as parameters
     *                   for one or more executions of the script
     * @throws SQLException Thrown if the script cannot execute in full
     */
    public void execute(List<Object[]> parameters) throws SQLException
    {
        Database.execute( this.formQuery().setBatchParameters( parameters ), this.isHighPriority );
    }

    /**
     * Runs the script in the database
     * @throws SQLException Thrown if execution fails
     */
    public void execute() throws SQLException
    {
        Database.execute( this.formQuery(), this.isHighPriority );
    }

    /**
     * Retrieves a single value, denoted by the given label, using the script
     * @param label The name of the column that contains the value
     * @param <V> The type of value to return
     * @return The retrieved value
     * @throws SQLException Thrown if the value could not be retrieved
     */
    public <V> V retrieve(String label) throws SQLException
    {
        return Database.retrieve( this.formQuery(), label, this.isHighPriority );
    }

    public DataProvider getData(Object... parameters) throws SQLException
    {
        return Database.getData( this.formQuery().setParameters( parameters ), this.isHighPriority );
    }

    /**
     * Retrieves a single value, denoted by the given label, asynchronously
     * using the script
     * @param label The name of the column containing the value
     * @param <V> The type of the value to retrieve
     * @return The task that retrieves the value
     */
    public <V> Future<V> submit(final String label)
    {
        return Database.submit( this.formQuery(), label, this.isHighPriority );
    }

    /**
     * Retrieves the described data in a fully populated data provider
     * @return A DataSet containing all returned values
     * @throws SQLException Thrown if the DataSet could not be created
     */
    public DataProvider getData() throws SQLException
    {
        return Database.getData( this.formQuery(), this.isHighPriority );
    }

    public DataProvider buffer() throws SQLException
    {
        return Database.buffer( this.formQuery(), this.isHighPriority );
    }

    /**
     * Runs a consumer function on each row of the result returned from the script
     * <p>
     *     <b>Arguments are not used.</b>
     * </p>
     *
     * @param consumer A function that will use each row of the result set
     * @throws SQLException Thrown if the consumer threw an error
     * @throws SQLException Thrown if the script failed to run properly
     */
    public void consume(ExceptionalConsumer<DataProvider, SQLException> consumer) throws SQLException
    {
        Database.consume( this.formQuery( ), consumer, this.isHighPriority );
    }

    /**
     * Transforms each row of the result of a script into an object
     * <p>
     *     <b>Arguments are not used</b>
     * </p>
     *
     * @param interpretor The function that will convert a row into an object
     * @param <U> The type of object that will be returned
     * @return A list of transformed objects
     * @throws SQLException Thrown if the script is not correctly formed
     * @throws SQLException Thrown if the results cannot be correctly interpretted
     */
    public <U> List<U> interpret( ExceptionalFunction<DataProvider, U, SQLException> interpretor) throws SQLException
    {
        return Database.interpret( this.formQuery(), interpretor, this.isHighPriority );
    }

    private Query formQuery()
    {
        Query query = Query.withScript( this.toString() )
                           .inTransaction( this.useTransaction )
                           .lockTables( this.lockTables );

        if (!this.arguments.isEmpty())
        {
            query.setParameters( this.arguments.toArray() );
        }

        return query;
    }
}

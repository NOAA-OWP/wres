package wres.io.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.ValueRetriever;
import wres.util.functional.ExceptionalConsumer;
import wres.util.functional.ExceptionalFunction;

public class DataScripter extends ScriptBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DataScripter.class );
    private boolean isHighPriority = false;
    private final List<Object> arguments = new ArrayList<>(  );

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

    public void addArgument(final Object argument)
    {
        this.arguments.add(argument);
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
        List<Object[]> parameterList = new ArrayList<>(  );
        parameterList.add( parameters );

        this.execute(parameterList );
    }

    /**
     * Executes the script in batch with the given parameters
     * @param parameters A collection of sets of objects to use as parameters
     *                   for one or more executions of the script
     * @throws SQLException Thrown if the script cannot execute in full
     */
    public void execute(List<Object[]> parameters) throws SQLException
    {
        Connection connection = null;
        PreparedStatement statement = null;

        try
        {
            if (this.isHighPriority)
            {
                connection = Database.getHighPriorityConnection();
            }
            else
            {
                connection = Database.getConnection();
            }

            statement = this.getPreparedStatement( connection, parameters );
            statement.execute();
        }
        finally
        {
            if (statement != null)
            {
                try
                {
                    statement.close();
                }
                catch (SQLException exception)
                {
                    LOGGER.debug("Failed to close the prepared statement with the script:{}{}",
                                 NEWLINE,
                                 this.toString());
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }
    }

    /**
     * Runs the script in the database
     * @throws SQLException Thrown if execution fails
     */
    public void execute() throws SQLException
    {
        // TODO: Add support for high priority connections
        if (this.arguments.isEmpty())
        {
            Database.execute( this.toString() );
        }
        else
        {
            List<Object[]> batchArguments = new ArrayList<>(  );
            batchArguments.add( this.arguments.toArray() );
            Database.execute( this.toString(), batchArguments );
        }
    }

    /**
     * Forces the built query to execute within an explicit transaction
     * @throws SQLException Thrown if the query fails
     */
    public void executeInTransaction() throws SQLException
    {
        // TODO: Add support for high priority connections
        Database.execute( this.toString(), true );
    }

    /**
     * Runs the script asynchronously
     * @return The task that is running the script
     */
    public Future issue()
    {
        // TODO: Add support for high priority connections
        SQLExecutor executor = new SQLExecutor( this.toString() );
        return Database.execute( executor );
    }

    /**
     * Runs the script asynchronously in an explicit transaction
     * @return The task that is running the script
     */
    public Future issueTransaction()
    {
        // TODO: Add support for high priority connections
        SQLExecutor executor = new SQLExecutor( this.toString(), true );
        return Database.execute( executor );
    }

    /**
     * Runs the script with the given parameters asynchronously
     * @param parameters A collection of objects to use as parameters
     * @return The task that runs the script with the given parameters
     */
    public Future issue(Collection<Object> parameters)
    {
        return this.issue(parameters.toArray());
    }

    /**
     * Runs the script with the given parameters asynchronously
     * @param parameters A collection of objects to use as parameters
     * @return The task that runs the script with the
     * given parameters
     */
    public Future issue(Object... parameters)
    {
        List<Object[]> parameterList = new ArrayList<>(  );
        parameterList.add( parameters );

        return this.issue(parameterList);
    }

    /**
     * Runs the script in batch with the given parameters asynchronously
     * @param parameters A collection of sets of objects to use as parameters
     *                   for one or more executions of the script
     * @return The task that runs the script with the given parameters
     */
    public Future issue(List<Object[]> parameters)
    {
        // SQLExecutor needs support for high priority connections, although it isn't being used
        SQLExecutor executor = new SQLExecutor( this.toString() );
        executor.addBatchArguments( parameters );
        return Database.execute( executor );
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
        return Database.getResult( this.toString(), label, this.isHighPriority );
    }

    /**
     * Retrieves the described data through a streaming data provider
     * @param connection The connection that will facilitate the result streaming
     * @return The DataProvider containing the query results
     * @throws SQLException Thrown if the query cannot be executed
     */
    public DataProvider getData( Connection connection) throws SQLException
    {
        if (this.arguments.isEmpty())
        {
            return Database.getResults( connection, this.toString() );
        }

        return Database.getResults( connection, this.toString(), this.arguments.toArray() );
    }

    public DataProvider getData(Collection<Object> parameters) throws SQLException
    {
        return Database.getResults(
                this.toString(),
                parameters.toArray(),
                this.isHighPriority
        );
    }

    public DataProvider getData(Object... parameters) throws SQLException
    {
        return Database.getResults(
                this.toString(),
                parameters,
                this.isHighPriority
        );
    }

    /**
     * Retrieves a single value, denoted by the given label, asynchronously
     * using the script
     * @param label The name of the column containing the value
     * @param <V> The type of the value to retrieve
     * @return The task that retrieves the value
     */
    public <V> Future<V> submit(String label)
    {
        // ValueRetriever needs support for high priority connections
        ValueRetriever<V> retriever = new ValueRetriever<>( this.toString(), label );
        return Database.submit( retriever );
    }

    /**
     * Retrieves the described data in a fully populated data provider
     * @return A DataSet containing all returned values
     * @throws SQLException Thrown if the DataSet could not be created
     */
    public DataProvider getData() throws SQLException
    {
        if (this.arguments.isEmpty())
        {
            return Database.getData( this.toString(), this.isHighPriority );
        }
        else
        {
            return Database.getResults(this.toString(), this.arguments.toArray(), this.isHighPriority);
        }
    }

    /**
     * Creates a prepared statement using the script
     * @param connection The connection that the prepared statement will be run on
     * @return A prepared statement that will execute the script
     * @throws SQLException Thrown if the script could not be used to create
     * the statement
     */
    public PreparedStatement getPreparedStatement(final Connection connection)
            throws SQLException
    {
        if (this.arguments.isEmpty())
        {
            return connection.prepareStatement( this.toString() );
        }
        else
        {
            return this.getPreparedStatement( connection, this.arguments );
        }
    }

    /**
     * Creates a prepared statement using the script and loads values from the
     * collection of parameters into it
     * @param connection The connection that the prepared statement will be run
     * @param parameters A collection of objects to use as parameters
     * @return A prepared statement that will execute the script with the given
     * parameters
     * @throws SQLException Thrown if the script and parameters could not be
     * used to create a statement
     */
    public PreparedStatement getPreparedStatement(final Connection connection, Collection<Object> parameters)
            throws SQLException
    {
        return this.getPreparedStatement( connection, parameters.toArray() );
    }

    /**
     * Creates a prepared statement using the script and loads the given
     * parameters into it
     * @param connection The connection that the prepared statement will be run on
     * @param parameters A collection of objects to use as parameters
     * @return A prepared statement that will execute the script with the given
     * parameters
     * @throws SQLException Thrown if the script and parameters could not be
     * used to create a statement
     */
    public PreparedStatement getPreparedStatement(final Connection connection, Object... parameters)
            throws SQLException
    {
        List<Object[]> parameterList = new ArrayList<>(  );
        parameterList.add( parameters );

        return this.getPreparedStatement( connection, parameterList );
    }

    /**
     * Creates a prepared statement to run in batch using the script and loads
     * the given parameters
     * @param connection The connection that will run the script
     * @param parameters A collection of sets of parameters that will be used to
     *                   run the script one or more times
     * @return The prepared statement that will run the script with the given parameters
     * @throws SQLException Thrown if the script and parameters could not be
     * used to create a statement
     */
    public PreparedStatement getPreparedStatement(final Connection connection, List<Object[]> parameters)
            throws SQLException
    {
        PreparedStatement statement = this.getPreparedStatement( connection );

        for ( Object[] statementParameters : parameters )
        {
            int addedParameters = 0;
            try
            {
                for ( ; addedParameters < statementParameters.length; ++addedParameters )
                {
                    statement.setObject( addedParameters + 1, statementParameters[addedParameters] );
                }

                while ( addedParameters < statement.getParameterMetaData().getParameterCount() )
                {
                    statement.setObject( addedParameters + 1, null );
                }

                statement.addBatch();
            }
            catch ( SQLException e )
            {
                LOGGER.error("Prepared Statement could not be created.");
                LOGGER.error(this.toString());
                LOGGER.error( "Parameters:" );

                for (Object parameter : statementParameters)
                {
                    LOGGER.error("    " + String.valueOf( parameter ));
                }

                throw e;
            }
        }

        return statement;
    }

    /**
     * Runs a consumer function on each row of the result returned from the script
     * @param consumer A function that will use each row of the result set
     * @throws SQLException Thrown if the consumer threw an error
     * @throws SQLException Thrown if the script failed to run properly
     */
    public void consume(ExceptionalConsumer<DataProvider, SQLException> consumer) throws SQLException
    {
        if (this.isHighPriority)
        {
            Database.highPriorityConsume( this.toString(), consumer );
        }
        else
        {
            Database.consume( this.toString(), consumer );
        }
    }

    public <U> List<U> interpret( ExceptionalFunction<DataProvider, U, SQLException> interpretor) throws SQLException
    {
        return Database.interpret( this.toString(), interpretor, this.isHighPriority );
    }

}

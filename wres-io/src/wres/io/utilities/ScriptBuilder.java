package wres.io.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.StatementRunner;
import wres.io.concurrency.ValueRetriever;
import wres.util.functional.ExceptionalConsumer;
import wres.util.functional.ExceptionalFunction;

public class ScriptBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ScriptBuilder.class );
    private static final String NEWLINE = System.lineSeparator();
    private final StringBuilder script;
    private boolean isHighPriority = false;

    public ScriptBuilder()
    {
        this.script = new StringBuilder(  );
    }

    public ScriptBuilder (String beginning)
    {
        this.script = new StringBuilder( beginning );
    }

    /**
     * Sets whether or not the script should use high priority connections
     * @param highPriority Whether or not high priority connections should be used
     */
    public void setHighPriority(boolean highPriority)
    {
        this.isHighPriority = highPriority;
    }

    /**
     * Adds a collection of objects to the script
     * @param details a collection of objects whose string representations will
     *                be added to the script
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder add(Object... details)
    {
        for (Object detail : details)
        {
            this.script.append(detail);
        }

        return this;
    }

    /**
     * Ends the current line of the script
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addLine()
    {
        return this.add(NEWLINE);
    }

    /**
     * Adds a collection of objects to the scripts and ends the line
     * @param details A collection of objects whose string representations will
     *                be added to the script
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addLine(Object... details)
    {
        return this.add(details).addLine();
    }

    /**
     * Adds the specified number of tabs greater than 0 to the script
     *  <p>
     *      One tab is equivalent to four whitespace characters
     *  </p>
     * @param numberOfTabs The number of tabs to add to the script. If the
     *                     number is less than one, no tabs will be added.
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addTab(int numberOfTabs)
    {
        for ( int i = 0; i < numberOfTabs; i++ )
        {
            this.add("    ");
        }

        return this;
    }

    /**
     * Adds a single tab to the script
     *  <p>
     *      One tab is equivalent to four whitespace characters
     *  </p>
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addTab()
    {
        return addTab(1);
    }

    @Override
    public String toString()
    {
        return this.script.toString();
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
        Database.execute( this.toString());
    }

    /**
     * Forces the built query to execute within an explicit transaction
     * @throws SQLException Thrown if the query fails
     */
    public void executeInTransaction() throws SQLException
    {
        Database.execute( this.toString(), true );
    }

    /**
     * Runs the script asynchronously
     * @return The task that is running the script
     */
    public Future issue()
    {
        SQLExecutor executor = new SQLExecutor( this.toString() );
        return Database.execute( executor );
    }

    /**
     * Runs the script asynchronously in an explicit transaction
     * @return The task that is running the script
     */
    public Future issueTransaction()
    {
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
        StatementRunner runner = new StatementRunner( this.toString(), parameters );
        return Database.execute( runner );
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
        V value;
        Connection connection = null;

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

            value = Database.getResult( this.toString(), label );
        }
        finally
        {
            if (connection != null)
            {
                if (this.isHighPriority)
                {
                    Database.returnHighPriorityConnection( connection );
                }
                else
                {
                    Database.returnConnection( connection );
                }
            }
        }

        return value;
    }

    /**
     * Retrieves a ResultSet for query result streaming
     * @param connection The connection that will facilitate the result streaming
     * @return The ResultSet containing the query results
     * @throws SQLException Thrown if the query cannot be executed
     */
    public ResultSet retrieve( Connection connection) throws SQLException
    {
        return Database.getResults( connection, this.toString() );
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
        ValueRetriever<V> retriever = new ValueRetriever<>( this.toString(), label );
        return Database.submit( retriever );
    }

    /**
     * Retrieves all data from the script and presents it in a structure
     * divorced from the database itself.
     * @return A DataSet containing all returned values
     * @throws SQLException Thrown if the DataSet could not be created
     */
    public DataSet getData() throws SQLException
    {
        return Database.getDataSet( this.toString() );
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
        return connection.prepareStatement( this.toString() );
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
    public void consume(ExceptionalConsumer<ResultSet, SQLException> consumer) throws SQLException
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

    public <U> List<U> interpret( ExceptionalFunction<ResultSet, U, SQLException> interpretor) throws SQLException
    {
        return Database.interpret( this.toString(), interpretor, this.isHighPriority );
    }

}

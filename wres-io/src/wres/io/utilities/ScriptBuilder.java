package wres.io.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.StatementRunner;
import wres.io.concurrency.ValueRetriever;

public class ScriptBuilder
{
    private static final String NEWLINE = System.lineSeparator();
    private final StringBuilder script;

    public ScriptBuilder()
    {
        this.script = new StringBuilder(  );
    }

    public ScriptBuilder (String beginning)
    {
        this.script = new StringBuilder( beginning );
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
        Future task = this.issue(parameters);

        try
        {
            task.get();
        }
        catch ( InterruptedException e )
        {
            throw new SQLException( "Script execution was interrupted.", e );
        }
        catch ( ExecutionException e )
        {
            throw new SQLException( "An error occurred while executing the script.", e );
        }
    }

    /**
     * Runs the script in the database
     * @throws SQLException Thrown if execution fails
     */
    public void execute() throws SQLException
    {
        Future task = this.issue();

        try
        {
            task.get();
        }
        catch ( InterruptedException e )
        {
            throw new SQLException( "Script execution was interrupted.", e );
        }
        catch ( ExecutionException e )
        {
            throw new SQLException( "An error occurred while executing the script.", e );
        }
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
        Future<V> task = this.submit( label );

        try
        {
            value = task.get();
        }
        catch ( InterruptedException e )
        {
            throw new SQLException( "Script execution was interrupted.", e );
        }
        catch ( ExecutionException e )
        {
            throw new SQLException( "An error occurred while executing the script.", e );
        }

        return value;
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
        ValueRetriever<V> retriever = new ValueRetriever<V>( this.toString(), label );
        return Database.submit( retriever );
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

        for (Object[] statementParameters : parameters )
        {
            int addedParameters = 0;
            for (; addedParameters < statementParameters.length; ++addedParameters)
            {
                statement.setObject( addedParameters + 1, statementParameters[addedParameters] );
            }

            while (addedParameters < statement.getParameterMetaData().getParameterCount())
            {
                statement.setObject( addedParameters + 1, null );
            }

            statement.addBatch();
        }

        return statement;
    }

}

package wres.io.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.system.SystemSettings;
import wres.util.Strings;

/**
 * Facilitates the execution of Database Queries upon connections
 */
public class Query
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Query.class );
    private static final String NEWLINE = System.lineSeparator();

    /**
     * The amount of milliseconds before a script is logged in debug mode
     */
    private static final long TIMER_DELAY = 2000L;

    /**
     * The script to run as part of the query
     */
    private final String script;

    /**
     * Whether or not the query should be run in a transaction. If the connection is already in
     * a transaction, it will be respected
     */
    private boolean forceTransaction;

    /**
     * Parameters to use in a single query execution
     */
    private Object[] parameters;

    /**
     * A set of parameters to use for batch execution
     */
    private Collection<Object[]> batchParameters;

    /**
     * A set of tables that should be locked prior to execution
     */
    private Set<String> locKTables = new HashSet<>();

    /**
     * Constructor
     * @param script The script that the query will run
     */
    private Query(final String script)
    {
        this.script = script;
    }

    /**
     * Generates a query with the given script
     * @param script The script to run
     * @return The newly generated {@link Query} instance
     */
    static Query withScript( final String script)
    {
        return new Query( script );
    }

    /**
     * Informs the Query whether or not it should run in a transaction (default is false)
     * @param forceTransaction Whether or not the query should run within a transaction
     * @return The updated {@link Query}
     */
    Query inTransaction(final boolean forceTransaction)
    {
        this.forceTransaction = forceTransaction;
        return this;
    }

    /**
     * Set the parameters for a single parameterized execution
     * @param parameters Parameters to add to a script for execution
     * @return The updated {@link Query}
     * @throws IllegalArgumentException Thrown when batch parameters have already been set; the two are not compatible
     */
    public Query setParameters(final Object... parameters)
    {
        if (this.batchParameters != null)
        {
            throw new IllegalArgumentException(
                    "A single set of parameters cannot be set if there are already parameters to run in batch"
            );
        }

        this.parameters = parameters;
        return this;
    }

    /**
     * Set the parameters to load when running the script in batch
     * @param batchParameters The collection of parameters to use each time the script is run
     * @return The updated {@link Query}
     * @throws IllegalArgumentException Thrown if standard parameters have already been set; the two are not compatible
     */
    Query setBatchParameters(final Collection<Object[]> batchParameters)
    {
        if (this.parameters != null)
        {
            throw new IllegalArgumentException(
                    "Batch parameters cannot be set if there is already a single set of parameters to use."
            );
        }

        this.batchParameters = batchParameters;
        return this;
    }

    /**
     * Add a collection of table names that should be locked prior to running the script
     * @param tableNames A list of table names that should be locked prior to running the script
     * @return The updated {@link Query}
     */
    Query lockTables(final Collection<String> tableNames)
    {
        this.locKTables.addAll( tableNames );
        return this;
    }

    /**
     * Runs the query on the passed in connection and gathers the results
     * <br><br>
     * <p>
     *     The statement that called the script and the resulting {@link ResultSet} are left open, so make
     *     sure to close them when done to prevent a resource leak
     * </p>
     * @param connection The database connection to run the query on
     * @return The results of the query
     * @throws SQLException Thrown if an interaction with the database produced an error
     */
    ResultSet call(final Connection connection) throws SQLException
    {
        // Record the initial auto commit state. If we change this state, we want to
        // ensure that it returns to it after we're done. If a transactional connection is passed in
        // through multiple queries, we want to make sure the transaction doesn't close
        final boolean initialAutoCommit = connection.getAutoCommit();

        Timer timer = null;

        // If we're in debug mode, we want to add any scripts that take longer than TIMER_DELAY ms to complete
        if (LOGGER.isDebugEnabled())
        {
            timer = new Timer( "Query Timer" );
            timer.schedule( this.getTimerTask(), TIMER_DELAY );
        }

        final ResultSet results;

        try
        {
            // If we're forcing a transation, we need to be absolutely sure we enter a transaction,
            // even if it's already in one
            if (this.forceTransaction)
            {
                connection.setAutoCommit( false );
            }

            // If we have tables we need to lock, we need to go ahead and do that here before we run the query
            for ( String tableName : this.locKTables )
            {
                this.lockTable( connection, tableName );
            }

            // If we don't need to add parameters, we can just call the script and get the results
            if ( this.parameters == null)
            {
                results = this.simpleCall( connection );
            }
            else
            {
                // Otherwise we need to call a different function so that all parameters are added before
                // the script is called
                results = this.callWithParameters( connection );
            }

            // If the connection can't commit without prompting, we need to go ahead and do so manually
            if (!connection.getAutoCommit())
            {
                connection.commit();
            }
        }
        catch( SQLException exception )
        {
            // If the connection doesn't automatically rollback any changes, do so manually before
            // rethrowing the error
            if (!connection.getAutoCommit())
            {
                connection.rollback();
            }

            String message = "The script:" + NEWLINE;
            message += this.script + NEWLINE + NEWLINE;
            message += "Failed.";

            // Throw a new version of the exception with the script attached to it for easier debugging
            throw new SQLException(message, exception);
        }
        finally
        {
            // If the connection is in a transaction, we probably modified it, so we want to return it to
            // the previous state
            if (!connection.getAutoCommit())
            {
                connection.setAutoCommit( initialAutoCommit );
            }

            // If execution completed prior to the timer going off, we want to cancel it
            if ( timer != null )
            {
                timer.cancel();
            }
        }

        return results;
    }

    /**
     * Runs the script on the database without regard for results
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if an error was encountered when interacting with the database
     */
    public int execute(final Connection connection) throws SQLException
    {
        // Record the initial auto commit state. If we change this state, we want to
        // ensure that it returns to it after we're done. If a transactional connection is passed in
        // through multiple queries, we want to make sure the transaction doesn't close
        final boolean initialAutoCommit = connection.getAutoCommit();
        //final int initialTransactionIsolation = connection.getTransactionIsolation();

        Timer timer = null;
        int rowsModified;

        // If we're in debug mode, we want to add any scripts that take longer than TIMER_DELAY ms to complete
        if (LOGGER.isDebugEnabled())
        {
            timer = new Timer( "Query Timer" );
            timer.schedule( this.getTimerTask(), TIMER_DELAY );
        }

        try
        {
            // If we're forcing a transation, we need to be absolutely sure we enter a transaction,
            // even if it's already in one
            if (this.forceTransaction)
            {
                connection.setAutoCommit( false );
                //connection.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
            }

            // If we have tables we need to lock, we need to go ahead and do that here before we run the query
            for ( String tableName : this.locKTables )
            {
                this.lockTable( connection, tableName );
            }

            // If we have batch parameters, we need to call a specialized function that will form and attach
            // them to the statement that is run in the database
            if ( this.batchParameters != null )
            {
                rowsModified = this.batchExecute( connection );
            }
            else if ( this.parameters != null )
            {
                // If we have parameters for just a single call, we need to call a separate function to attach
                // them to the statement that is run in the database
                rowsModified = this.executeWithParameters( connection );
            }
            else
            {
                // Otherwise the script may be run in the database without any extra handling
                rowsModified = this.executeQuery( connection );
            }

            // If no rows were modified, the returning value will be -1, so set the number of modified rows
            // to 0 if that were the case
            rowsModified = Math.max( rowsModified, 0 );

            // If the connection can't commit without prompting, we need to go ahead and do so manually
            if (!connection.getAutoCommit())
            {
                connection.commit();
            }
        }
        catch (SQLException exception)
        {
            // If the connection doesn't automatically rollback any changes, do so manually before
            // rethrowing the error
            if (!connection.getAutoCommit())
            {
                connection.rollback();
            }

            String message = "The script:" + NEWLINE;
            message += this.script + NEWLINE + NEWLINE;
            message += "Failed.";

            // Throw a new version of the exception with the script attached to it for easier debugging
            throw new SQLException(message, exception);
        }
        finally
        {
            // Reset the transaction isolation level to its previous state
            //if ( connection.getTransactionIsolation() != initialTransactionIsolation )
            //{
            //    connection.setTransactionIsolation( initialTransactionIsolation );
            //}

            // If the connection is in a transaction, we probably modified it, so we want to return it to
            // the previous state
            if (!connection.getAutoCommit())
            {
                connection.setAutoCommit( initialAutoCommit );
            }

            // If execution completed prior to the timer going off, we want to cancel it
            if ( timer != null )
            {
                timer.cancel();
            }
        }

        return rowsModified;
    }

    /**
     * Runs the script with each set of parameters
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if the prepared statement required to run the script could not be created
     * @throws SQLException Thrown if an error was encountered when running the script in the database
     */
    private int batchExecute(final Connection connection) throws SQLException
    {
        int rowsModified = 0;
        // We need to make sure that the statement is cleaned up after execution
        try(PreparedStatement statement = this.prepareStatement( connection ))
        {
            int[] updates = statement.executeBatch();

            for (int update : updates)
            {
                rowsModified += update;
            }
        }

        return rowsModified;
    }

    /**
     * Run the script once with the given parameters
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if the prepared statement required to run the script could not be created
     * @throws SQLException Thrown if an error was encountered when running the script in the database
     */
    private int executeWithParameters(final Connection connection) throws SQLException
    {
        int rowsModified = 0;

        // We need to make sure that the statement is cleaned up after execution
        try(PreparedStatement preparedStatement = this.prepareStatement( connection ))
        {
            boolean generatedResultSet = preparedStatement.execute();

            if (generatedResultSet)
            {
                ResultSet executionResults = preparedStatement.getResultSet();

                while (executionResults.next())
                {
                    rowsModified++;
                }
            }
            else
            {
                rowsModified = preparedStatement.getUpdateCount();
            }
        }

        return rowsModified;
    }

    /**
     * Run the script with no extra handling
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if the statement required to run the script could not be created
     * @throws SQLException Thrown if an error was encountered when running the script in the database
     */
    private int executeQuery(final Connection connection) throws SQLException
    {
        int modifiedRows = 0;
        // We need to make sure that the statement is cleaned up after execution
        try (Statement statement = this.createStatement( connection ))
        {
            boolean generatedResultSet = statement.execute( this.script );

            if (generatedResultSet)
            {
                ResultSet executionResults = statement.getResultSet();

                while (executionResults.next())
                {
                    modifiedRows++;
                }
            }
            else
            {
                modifiedRows = statement.getUpdateCount();
            }
        }

        return modifiedRows;
    }

    /**
     * Call the script with the passed in parameters
     * <br><br>
     *     <p>
     *         The statement that retrieves the results is left open to keep the {@link ResultSet} open
     *     </p>
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if the prepared statement required to run the script could not be created
     * @throws SQLException Thrown if an error was encountered when running the script in the database
     */
    private ResultSet callWithParameters(final Connection connection) throws SQLException
    {
        PreparedStatement statement = this.prepareStatement( connection );
        return statement.executeQuery();
    }

    /**
     * Call the script with no extra handling
     * <br><br>
     *     <p>
     *         The statement that retrieves the results is left open to keep the {@link ResultSet} open
     *     </p>
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if the statement required to run the script could not be created
     * @throws SQLException Thrown if an error was encountered when running the script in the database
     */
    private ResultSet simpleCall(final Connection connection) throws SQLException
    {
        return this.createStatement( connection ).executeQuery( this.script );
    }

    /**
     * Locks the given table to prevent modification while the Query runs
     * @param connection The connection that will own the lock
     * @param tableName The name of the table to lock ('schema.Table')
     * @throws SQLException Thrown if the statement required to run the lock instruction could not be created
     * @throws SQLException Thrown if an error was encountered when running the lock instruction in the database
     */
    private void lockTable(final Connection connection, final String tableName) throws SQLException
    {
        // Until we have a better process, this is the best way to tell if we have a postresql connection
        String connectionName = (String)connection.getClientInfo().getOrDefault( "ApplicationName", "Unknown" );

        if (!connectionName.equalsIgnoreCase( "postgresql jdbc driver" ))
        {
            LOGGER.warn( "Explicit table lock operations are only valid in Postgresql databases. "
                         + "The {} table could not be locked.", tableName );
            return;
        }
        // If autocommit is turned off, we want to lock the table in question. Otherwise,
        // the operation is pointless and we'll want to skip it
        if (!connection.getAutoCommit())
        {
            String lockQuery = "LOCK TABLE " + tableName + " IN ACCESS EXCLUSIVE MODE;";
            try (Statement statement = this.createStatement( connection ))
            {
                statement.execute( lockQuery );
            }
            catch ( SQLException exception )
            {
                String message = "The table '" + tableName + "' could not be locked."
                                 + NEWLINE + "The query was: '" + lockQuery + "'";
                // Decorate SQLException with contextual information
                throw new SQLException( message, exception );
            }
        }
        else
        {
            LOGGER.debug( "The application is seeking to lock a table with a "
                          + "connection that will automatically commit. Nothing "
                          + "will be achieved by this action." );
            // We need to know how we got here, but, since the stack trace from the immediate thread
            // isn't exactly helpful, we use the helper function to make it readable in the logs
            LOGGER.debug( Strings.getStackTrace() );
        }
    }

    /**
     * Creates a standard statement that doesn't need to consider parameters
     * @param connection The connection that the script will run on
     * @return A simple statement object to run the script on
     * @throws SQLException Thrown if a statement couldn't be created on the connection
     */
    private Statement createStatement(final Connection connection) throws SQLException
    {
        // All system-wide database connection settings should be added here
        Statement statement = connection.createStatement();
        statement.setQueryTimeout( SystemSettings.getQueryTimeout() );
        return statement;
    }

    /**
     * Creates a prepared statement that will take parameters into account when running a script
     * @param connection  The connection that the script will run on
     * @return A prepared statement that will run a query with the parameters added to the class
     * @throws SQLException Thrown if the prepared statement could not be created
     * @throws SQLException Thrown if the parameters could not be added to the prepared statement
     */
    private PreparedStatement prepareStatement(final Connection connection) throws SQLException
    {
        // All system-wide database connection settings should be added here
        PreparedStatement statement = connection.prepareStatement( this.script );
        statement.setQueryTimeout( SystemSettings.getQueryTimeout() );

        // If we have a basic array of parameters, we can just add them directly to the statement
        if (this.parameters != null)
        {
            this.addParametersToStatement( statement, this.parameters );
        }
        else if (this.batchParameters != null)
        {
            // If we have batch parameters, we'll need to add them set by set and add the combinations to the
            // statement to be run separately
            for ( Object[] statementValues : this.batchParameters )
            {
                this.addParametersToStatement( statement, statementValues );

                statement.addBatch();
            }
        }

        return statement;
    }

    /**
     * Adds the passed in collection of parameters to the given statement
     * @param statement The statement that needs the set of parameters
     * @param parameters The parameters to add to the statement
     * @throws SQLException Thrown if a parameter could not be added to the query
     * @throws SQLException Thrown if the number of parameters in the script could not be detected
     */
    private void addParametersToStatement(final PreparedStatement statement, final Object[] parameters)
            throws SQLException
    {
        // We need to keep track of the number of parameters added outside of the loop
        int addedParameters = 0;

        // Add each parameter as a generic object to the statement as parameters
        for ( ; addedParameters < parameters.length; ++addedParameters )
        {
            // SQL parameters are 1's indexed, so we need to adjust to make sure they get in the right position
            statement.setObject( addedParameters + 1, parameters[addedParameters] );
        }

        // JDBC can detect the parameters that need to be added when running a script. If there are 8 in the
        // script, but only 6 passed in, we need to fill in the gaps. This is a case that shows up every once in a
        // while and isn't invalid.
        while ( addedParameters < statement.getParameterMetaData().getParameterCount() )
        {
            // Again, adjust for the 1's indexing.
            // Set the missing parameter as null since we have nothing that we can infer
            statement.setObject( addedParameters + 1, null );
            addedParameters++;
        }
    }

    /**
     * Creates a task that will log a query and cancel itself, preventing it from happening for a second time
     * @return A new task to place in a timer
     */
    private TimerTask getTimerTask()
    {
        return new TimerTask() {
            @Override
            public void run()
            {
                LOGGER.debug(
                        "A long running query has been encountered:{}{}",
                        NEWLINE,
                        this.query
                );
                this.cancel();
            }

            TimerTask init(final String query)
            {
                this.query = query;
                return this;
            }

            private String query;
        }.init( this.script );
    }
}

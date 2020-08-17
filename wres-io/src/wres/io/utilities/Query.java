package wres.io.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

import wres.system.SystemSettings;

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

    private final SystemSettings systemSettings;

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
     * Whether to use a cursor to fetch results, i.e. stream/buffer resultset.
     */
    private boolean useCursor;

    /**
     * Parameters to use in a single query execution
     */
    private Object[] parameters;

    /**
     * A list of parameters to use for batch execution
     */
    private List<Object[]> batchParameters;


    /**
     * A set of SQLStates that should cause indefinite retry.
     */
    private Set<String> sqlStatesToRetry = Collections.emptySet();

    /**
     * A surrogate int key created by an insert, available to read only after
     * running "execute" that returns rows affected.
     */

    private List<Long> insertedIds = Collections.emptyList();


    /**
     * Whether the Query has multiple statements, auto-detected on construction
     * by the presence of a semicolon with text following it.
     * When there are multiple statements, no prepared statements are supported,
     * the first N-1 statements will be executed as a batch, the last statement
     * is assumed to have a ResultSet, and only the "call()" method supports
     * multipleStatements, and the inserted ids are not kept as of 2020-08-14.
     */

    private final boolean hasMultipleStatements;

    /**
     * Each statement for multiple-statement queries, populated on construction
     * when multiple statements are detected or null when only one statement is
     * detected. Use boolean multipleStatements before looking here.
     */

    private final String[] statements;

    /**
     * Constructor
     * @param script The script that the query will run
     */
    Query( SystemSettings systemSettings,
           final String script )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( script );

        if ( script.isBlank() )
        {
            throw new IllegalArgumentException( "Non-blank script required." );
        }

        final int MIN_QUERY_CHARS = 5;

        if ( script.length() <= MIN_QUERY_CHARS )
        {
            throw new IllegalArgumentException( "Expected SQL script over 4 chars, got: "
                                                + script );
        }

        this.systemSettings = systemSettings;
        this.script = script;

        // Detect multiple statements by looking at semi-colons.
        // First check of char-within-string should be faster than regex etc.
        int indexOfFirstSemicolon = script.indexOf( ';' );

        // If a semicolon exists and the semicolon is not the last char of the
        // whole script, check further.
        if ( indexOfFirstSemicolon > 0
             && indexOfFirstSemicolon < script.length() - 1 )
        {
            // Now more sophisticated check required. Suppose whitespace occurs
            // to either side of a semi-colon with no other chars. That is not
            // multiple statements, then.
            String[] chunks = script.split( ";" );
            List<String> statements = new ArrayList<>( chunks.length );

            for ( String chunk : chunks )
            {
                String stripped = chunk.strip();

                if ( stripped.length() >= MIN_QUERY_CHARS
                     && !stripped.startsWith( "'" )
                     && !stripped.endsWith( "'" ) )
                {
                    statements.add( chunk );
                }
                else
                {
                    LOGGER.debug( "Treating the given query as single query." );
                    statements.clear();
                    break;
                }
            }

            if ( statements.size() > 1 )
            {
                this.hasMultipleStatements = true;
                this.statements = statements.toArray( new String[0] );
            }
            else
            {
                // After a closer look, there is actually only one statement.
                this.hasMultipleStatements = false;
                this.statements = null;
            }
        }
        else
        {
            // After a minimal look, there is only one statement.
            this.hasMultipleStatements = false;
            this.statements = null;
        }
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

    Query useCursor( boolean useCursor )
    {
        this.useCursor = useCursor;
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

        if ( this.hasMultipleStatements )
        {
            throw new UnsupportedOperationException( "Parameters not supported for multi-statement queries." );
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
    Query setBatchParameters(final List<Object[]> batchParameters)
    {
        if (this.parameters != null)
        {
            throw new IllegalArgumentException(
                    "Batch parameters cannot be set if there is already a single set of parameters to use."
            );
        }

        if ( this.hasMultipleStatements )
        {
            throw new UnsupportedOperationException( "Batch parameters not supported for multi-statement queries." );
        }

        this.batchParameters = batchParameters;
        return this;
    }


    /**
     * Add a SQLState that should cause indefinite retry instead of SQLException
     * @param sqlStateToRetry The sqlState to tolerate, five digit alphanumeric
     * @return The updated {@link Query}
     * @throws IllegalArgumentException When sqlState isn't 5 digit alphanumeric
     * @throws NullPointerException When sqlState is null
     */

    Query retryOnSqlState( String sqlStateToRetry )
    {
        Objects.requireNonNull( sqlStateToRetry );

        if ( sqlStateToRetry.length() != 5
             || sqlStateToRetry.getBytes().length != 5 )
        {
            throw new IllegalArgumentException( "Valid SQLSTATE String is exactly five digits and five bytes. "
                                                + sqlStateToRetry + " is "
                                                + sqlStateToRetry.length()
                                                + " digits and "
                                                + sqlStateToRetry.getBytes().length );
        }

        char[] sqlStateCharacters = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                // There is no 'I' in SQLSTATE codes.
                'J', 'K', 'L', 'M', 'N',
                // There is no 'O' in SQLSTATE codes.
                'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
        };
        // To guarantee that the search will work in a moment.
        Arrays.sort( sqlStateCharacters );

        for ( char c : sqlStateToRetry.toUpperCase()
                                      .toCharArray() )
        {
            if ( Arrays.binarySearch( sqlStateCharacters, c ) < 0 )
            {
                throw new IllegalArgumentException(
                        "Valid SQLSTATE String only contains particular Roman letters and Arabic numbers, not "
                        + c + ". Please correct argument "
                        + sqlStateToRetry );
            }
        }

        if ( this.sqlStatesToRetry.equals( Collections.emptySet() ) )
        {
            // Set to two because the only callers known (as of 2019-05-23) will
            // use exactly two conditions: unique constraint violation and
            // serialization failure.
            this.sqlStatesToRetry = new HashSet<>( 2 );
        }

        this.sqlStatesToRetry.add( sqlStateToRetry );
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
     * @return The ResultSet of the query
     * @throws SQLException Thrown if an interaction with the database produced an unrecoverable error
     */
    ResultSet call(final Connection connection) throws SQLException
    {
        // Avoid recording the initial state of connection: reduces round trips.
        final boolean disableAutoCommit = this.forceTransaction || this.useCursor;
        final boolean useSerializable = this.forceTransaction;
        final boolean multipleStatements = this.script.startsWith( "CREATE TEMPORARY" );

        // In the case of transactions that conflict, retries are needed.
        boolean completed = false;

        Timer timer = null;

        connection.setAutoCommit( !disableAutoCommit );

        if ( useSerializable )
        {
            connection.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
        }
        else
        {
            connection.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
        }

        ResultSet results = null;

        while ( !completed )
        {
            // If we're in debug mode, we want to add any scripts that take longer than TIMER_DELAY ms to complete
            if ( LOGGER.isDebugEnabled() )
            {
                timer = new Timer( "Query Timer" );
                timer.schedule( this.getTimerTask(), TIMER_DELAY );
            }

            try
            {
                if ( this.useCursor )
                {
                    LOGGER.debug( "Creating ResultSet using cursor {}", this );
                }

                // If we don't need to add parameters, we can just call the script and get the results
                if ( this.parameters == null )
                {
                    // Common case first, only single statement. Do simpleCall.
                    if ( !this.hasMultipleStatements )
                    {
                        results = this.simpleCall( connection );
                    }
                    else
                    {
                        Statement statement = this.createStatement( connection );
                        int lastStatement = this.statements.length - 1;
                        String[] allButLastStatement = Arrays.copyOfRange( this.statements, 0, lastStatement );
                        int result = this.executeBatch( statement, allButLastStatement );
                        LOGGER.debug( "Result of multiple statements? {}",
                                      result );
                        boolean resultTwo = statement.execute( this.statements[lastStatement] );

                        if ( !resultTwo )
                        {
                            throw new SQLException( "Expected statement to have a ResultSet but it did not." );
                        }

                        ResultSet resultSet = statement.getResultSet();
                        LOGGER.debug( "Found these results: {}",
                                      resultSet );
                        results = resultSet;
                    }
                }
                else
                {
                    // Otherwise we need to call a different function so that all parameters are added before
                    // the script is called
                    results = this.callWithParameters( connection );
                }

                // If the connection can't commit without prompting, we need to go ahead and do so manually
                // Except when leaving a ResultSet open, using a cursor.
                if ( disableAutoCommit && !this.useCursor )
                {
                    connection.commit();
                }

                completed = true;
            }
            catch ( SQLException exception )
            {
                String sqlState = exception.getSQLState();

                LOGGER.debug( "SQLState: {}, Connection: {}, this: {}",
                              sqlState,
                              connection,
                              this,
                              exception );

                // If the connection doesn't automatically rollback any changes, do so manually before
                // rethrowing the error
                if ( !connection.isClosed()
                     && disableAutoCommit )
                {
                    connection.rollback();
                }

                // In the case of specified retry conditions, retry.
                if ( this.sqlStatesToRetry.contains( sqlState.toUpperCase() ) )
                {
                    LOGGER.debug( "Got SQLState {}, retrying {}",
                                  sqlState,
                                  this );
                    continue;
                }

                String message = "The Query: " + NEWLINE;
                message += this + NEWLINE + NEWLINE;
                message += "Failed.";

                // Throw a new version of the exception with the script attached to it for easier debugging
                throw new SQLException( message, exception );
            }
            finally
            {
                // Instead of examining and resetting connection state with each
                // query, always set the desired state before each query.

                // If execution completed prior to the timer going off, we want to cancel it
                if ( timer != null )
                {
                    timer.cancel();
                }
            }
        }

        return results;
    }

    /**
     * Runs the script on the database without regard for results
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if an unrecoverable error was encountered when interacting with the database
     * @throws IllegalStateException When asked to use a cursor with this method.
     * @return the count of rows modified by this query's execution
     */

    public int execute(final Connection connection) throws SQLException
    {
        if ( this.useCursor )
        {
            throw new IllegalStateException( "It does not make sense to 'execute' with a cursor. Use 'call' instead." );
        }

        if ( this.hasMultipleStatements )
        {
            throw new UnsupportedOperationException( "Only 'call' method is supported for multi-statement queries. (Feel free to add 'execute' support if needed." );
        }

        // Avoid recording the initial state of connection: reduces round trips.
        final boolean disableAutoCommit = this.forceTransaction;
        final boolean useSerializable = this.forceTransaction;

        // In the case of transactions that conflict, retries are needed.
        boolean completed = false;

        Timer timer = null;
        int rowsModified = 0;

        connection.setAutoCommit( !disableAutoCommit );

        if ( useSerializable )
        {
            connection.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
        }
        else
        {
            connection.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
        }

        while ( !completed )
        {
            // If we're in debug mode, we want to add any scripts that take longer than TIMER_DELAY ms to complete
            if ( LOGGER.isDebugEnabled() )
            {
                timer = new Timer( "Query Timer" );
                timer.schedule( this.getTimerTask(), TIMER_DELAY );
            }

            try
            {
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
                if ( disableAutoCommit )
                {
                    connection.commit();
                }

                completed = true;
            }
            catch ( SQLException exception )
            {
                String sqlState = exception.getSQLState();

                LOGGER.debug( "SQLState: {}, Connection: {}, this: {}",
                                  sqlState,
                                  connection,
                                  this,
                                  exception );

                // If the connection doesn't automatically rollback any changes, do so manually before
                // rethrowing the error
                if ( !connection.isClosed()
                     && disableAutoCommit )
                {
                    connection.rollback();
                }

                // In the case of specified retry conditions, retry.
                if ( this.sqlStatesToRetry.contains( sqlState.toUpperCase() ) )
                {
                    LOGGER.debug( "Got SQLState {}, retrying {}",
                                  sqlState,
                                  this );
                    continue;
                }

                String message = "The script:" + NEWLINE;
                message += this.script + NEWLINE + NEWLINE;
                message += "Failed.";

                // Throw a new version of the exception with the script attached to it for easier debugging
                throw new SQLException( message, exception );
            }
            finally
            {
                // Instead of examining and resetting connection state with each
                // query, always set the desired state before each query.

                // If execution completed prior to the timer going off, we want to cancel it
                if ( timer != null )
                {
                    timer.cancel();
                }
            }
        }

        return rowsModified;
    }

    /**
     * Runs the script with each set of parameters
     * @param connection The connection to run the script on
     * @return The number of modified rows
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
     * @throws UnsupportedOperationException when a ResultSet is generated
     * @return the count of rows modified by this query's execution
     */
    private int executeWithParameters(final Connection connection) throws SQLException
    {
        int modifiedRows;

        // We need to make sure that the statement is cleaned up after execution
        try(PreparedStatement preparedStatement = this.prepareStatement( connection ))
        {
            boolean generatedResultSet = preparedStatement.execute();

            if (generatedResultSet)
            {
                throw new UnsupportedOperationException(
                        "Results from queries may not be formed through execute statements. Use call instead."
                );
            }

            modifiedRows = preparedStatement.getUpdateCount();

            // When rows have been modified, attempt to get the first
            // auto-generated int key from the first row inserted.
            if ( modifiedRows > 0 )
            {
                this.insertedIds = new ArrayList<>();

                try ( ResultSet keySet = preparedStatement.getGeneratedKeys() )
                {
                    ResultSetMetaData metaData = keySet.getMetaData();
                    int columnType = metaData.getColumnType( 1 );
                    LOGGER.debug( "Column type of keys returned: {}",
                                  columnType );

                    if ( columnType == Types.BIGINT
                         || columnType == Types.INTEGER
                         || columnType == Types.SMALLINT
                         || columnType == Types.TINYINT )
                    {
                        while ( keySet.next() )
                        {
                            // All ints from tiny to big can fit in a long
                            this.insertedIds.add( keySet.getLong( 1 ) );
                        }

                        if ( this.insertedIds.size() > 0 )
                        {
                            LOGGER.debug( "Found an inserted id for Query {}.",
                                          this );
                        }
                    }
                    else
                    {
                        LOGGER.debug( "No integer values returned for {}",
                                      this );
                    }
                }
            }
            else
            {
                LOGGER.debug( "No modified rows for Query {}.", this );
            }
        }

        return modifiedRows;
    }

    /**
     * Run the script with no extra handling
     * @param connection The connection to run the script on
     * @throws SQLException Thrown if the statement required to run the script could not be created
     * @throws SQLException Thrown if an error was encountered when running the script in the database
     * @throws UnsupportedOperationException when a ResultSet is generated
     * @return the count of rows modified by this query's execution
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
                throw new UnsupportedOperationException(
                        "Results from queries may not be formed through execute statements. Use call instead."
                );
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
     * Add and execute the included statements as a single batch.
     *
     *     <p>
     *         The statement that retrieves the results is left open to keep
     *         the {@link ResultSet} open
     *     </p>
     * @param jdbcStatement The Statement to run the batch of statements on
     * @throws SQLException When anything goes wrong.
     */
    private int executeBatch( Statement jdbcStatement, String[] sqlStatements )
            throws SQLException
    {
        for ( String singleSqlStatement : sqlStatements )
        {
            jdbcStatement.addBatch( singleSqlStatement );
        }

        int[] countAffected = jdbcStatement.executeBatch();
        jdbcStatement.clearBatch();

        int total = 0;

        for ( int count : countAffected )
        {
            total += count;
        }

        return total;
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
        statement.setQueryTimeout( systemSettings.getQueryTimeout() );

        if ( this.useCursor )
        {
            statement.setFetchSize( systemSettings.fetchSize() );
        }

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
        PreparedStatement statement = connection.prepareStatement( this.script,
                                                                   RETURN_GENERATED_KEYS );
        statement.setQueryTimeout( systemSettings.getQueryTimeout() );

        if ( this.useCursor )
        {
            statement.setFetchSize( systemSettings.fetchSize() );
        }

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

    List<Long> getInsertedIds()
    {
        return this.insertedIds;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "script", script )
                .append( "forceTransaction", forceTransaction )
                .append( "useCursor", useCursor )
                .append( "parameters", parameters )
                .append( "batchParameters", batchParameters )
                .append( "sqlStatesToRetry", sqlStatesToRetry )
                .append( "insertedIds", insertedIds )
                .toString();
    }
}

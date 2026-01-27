package wres.io.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import wres.datamodel.DataProvider;

import java.util.IllegalFormatException;

/**
 * A class to help with running scripts against a database.
 */

public class DataScripter extends ScriptBuilder
{
    private final Database database;
    private boolean isHighPriority = false;
    private final List<Object> arguments = new ArrayList<>();
    private boolean useTransaction;
    private Set<String> sqlStatesToRetry = Collections.emptySet();
    private List<Long> insertedIds;
    private int maxRows;

    /**
     * Creates an instance.
     * @param database the database
     */
    public DataScripter( Database database )
    {
        super();
        this.database = database;
    }

    /**
     * @param database the database
     * @param beginning the beginning of the script
     */

    public DataScripter( Database database, final String beginning )
    {
        super( beginning );
        this.database = database;
    }

    /**
     * Sets whether or not the script should use high priority connections
     * @param highPriority Whether or not high priority connections should be used
     */
    public void setHighPriority( boolean highPriority )
    {
        this.isHighPriority = highPriority;
    }

    /**
     * Sets whether or not to run the query within a single transaction
     * @param useTransaction Whether or not to run the query within a transaction
     */
    public void setUseTransaction( boolean useTransaction )
    {
        this.useTransaction = useTransaction;
    }

    /**
     * Adds an argument to insert into the script while running the query in the database
     * @param argument The argument to run in the database
     * @return The updated DataScripter
     */
    public DataScripter addArgument( final Object argument )
    {
        this.arguments.add( argument );

        return this;
    }

    /**
     * Retries on encountering a serialization failure.
     */
    public void retryOnSerializationFailure()
    {
        if ( this.sqlStatesToRetry.equals( Collections.emptySet() ) )
        {
            this.sqlStatesToRetry = new HashSet<>( 2 );
        }

        this.sqlStatesToRetry.add( "40001" );
    }

    /**
     * Retries on encountering a unique constraint violation.
     */
    public void retryOnUniqueViolation()
    {
        if ( this.sqlStatesToRetry.equals( Collections.emptySet() ) )
        {
            this.sqlStatesToRetry = new HashSet<>( 2 );
        }

        this.sqlStatesToRetry.add( "23505" );
    }

    /**
     * Sets jdbc maximum rows AND adds a LIMIT clause if supported by database.
     * Order matters, expect this to put a LIMIT clause into your statement at
     * the position you call this method.
     * @param maxRows The maximum rows to fetch.
     */

    public void setMaxRows( int maxRows )
    {
        if ( maxRows <= 0 )
        {
            throw new IllegalArgumentException( "Expected > 0, got " + maxRows );
        }

        if ( this.database.getType()
                .isLimitClauseSupported() )
        {
            this.addLine( "LIMIT " + maxRows );
        }

        this.maxRows = maxRows;
    }

    /**
     * Executes the built script with the given parameters
     * @param parameters The values to use as parameters to the built script
     * @return The number of modified rows
     * @throws SQLException Thrown if execution of the script fails
     */
    public int execute( Object... parameters ) throws SQLException
    {
        Query query = this.formQuery()
                          .setParameters( parameters );
        int rowsModified = database.execute( query, this.isHighPriority );
        this.insertedIds = query.getInsertedIds();
        return rowsModified;
    }

    /**
     * Executes the script in batch with the given parameters
     * @param parameters A collection of sets of objects to use as parameters
     *                   for one or more executions of the script
     * @return the total number of rows modified by the batch operation
     * @throws SQLException Thrown if the script cannot execute in full
     */
    public int execute( List<Object[]> parameters ) throws SQLException
    {
        Query query = this.formQuery()
                          .setBatchParameters( parameters );
        int rowsModified = database.execute( query, this.isHighPriority );
        this.insertedIds = query.getInsertedIds();
        return rowsModified;
    }

    /**
     * Runs the script in the database
     * @return The number of updated or returned rows
     * @throws SQLException Thrown if execution fails
     */
    public int execute() throws SQLException
    {
        Query query = this.formQuery();
        int rowsModified = database.execute( query, this.isHighPriority );
        this.insertedIds = query.getInsertedIds();
        return rowsModified;
    }

    /**
     * Retrieves a single value, denoted by the given label, using the script
     * @param label The name of the column that contains the value
     * @param <V> The type of value to return
     * @return The retrieved value
     * @throws SQLException Thrown if the value could not be retrieved
     */
    public <V> V retrieve( String label ) throws SQLException
    {
        return this.database.retrieve( this.formQuery(), label, this.isHighPriority );
    }

    /**
     * Retrieves data from the database based on the formed query, in memory
     * @param parameters A collection of parameters to pass into the formed query
     * @return The collection of data loaded from the database
     * @throws SQLException Thrown if an issue was encountered while communicating with the database
     */
    public DataProvider getData( Object... parameters ) throws SQLException
    {
        return this.database.getData( this.formQuery().setParameters( parameters ), this.isHighPriority );
    }

    /**
     * Schedules the query to run in the database asynchronously
     * @return The scheduled task
     */
    public Future<?> issue()
    {
        return this.database.issue( this.formQuery(), this.isHighPriority );
    }

    /**
     * Retrieves the described data in a fully populated data provider, in memory
     * @return A DataSet containing all returned values
     * @throws SQLException Thrown if the DataSet could not be created
     */
    public DataProvider getData() throws SQLException
    {
        return this.database.getData( this.formQuery(), this.isHighPriority );
    }

    /**
     * Retrieves the described data in a streaming data provider
     * The caller must provide the connection to use and close it along with
     * closing the DataProvider.
     * @param connection The connection to use for streaming data.
     * @return A DataSet containing all returned values
     * @throws SQLException Thrown if the DataSet could not be created
     */
    public DataProvider buffer( Connection connection ) throws SQLException
    {
        Query query = this.formQuery()
                          .useCursor( true );
        return this.database.buffer( connection, query );
    }

    /**
     * Get the first available id of the each inserted row from a previous
     * invocation of "execute" on this instance. 0 if none available.
     * @return The id of the first inserted row or 0 if none was available.
     */

    public List<Long> getInsertedIds()
    {
        return this.insertedIds;
    }

    /**
     * @return the parameters associated with the script.
     */

    private List<Object> getParameters()
    {
        return Collections.unmodifiableList( this.arguments );
    }

    /**
     * @return a string representation of the parameters, pretty printing long[] and Long[]
     */

    public List<String> getParameterStrings()
    {
        List<String> parameterStrings = new ArrayList<>();
        for ( Object next : this.getParameters() )
        {
            if ( next instanceof long[] v )
            {
                parameterStrings.add( Arrays.toString( v ) );
            }
            else if ( next instanceof Long[] v )
            {
                parameterStrings.add( Arrays.toString( v ) );
            }
            else
            {
                parameterStrings.add( next.toString() );
            }
        }

        return Collections.unmodifiableList( parameterStrings );
    }

    /**
     * <p>Returns a string representation of the script that is runnable on a database instance. If the script contains 
     * a prepared statement, then the parameters are added inline to the script.
     *
     * <p>This is a utility method to assist in logging scripts that can be copied from the log and executed against a 
     * database instance, in order to assist in debugging. It should not be used to obtain a script to run in code 
     * against a database. Instead, use the runnable methods in this class to directly execute the script instance. 
     * Examples of these methods are {@link #execute()} and {@link #buffer(Connection)}.
     *
     * <p>When inserting a parameter into a prepared statement, the string representation of the parameter is used.
     * There is currently no special treatment of arrays, for example.
     *
     * @return a runnable string representation of the script
     * @throws IllegalFormatException if the script could not be formatted
     */

    public String toStringRunnableForDebugPurposes()
    {
        // Already runnable?
        if ( this.arguments.isEmpty() )
        {
            return super.toString();
        }
        // Prepared statement, so add parameters inline
        else
        {
            String script = super.toString();
            script = script.replace( "\\?", "'%s'" );
            return String.format( script, this.arguments.toArray() );
        }
    }

    /**
     * Creates the query to run in the database based on the configured settings
     * @return The query to run
     */
    private Query formQuery()
    {
        Query query = new Query( this.database.getSystemSettings(), this.toString() )
                .inTransaction( this.useTransaction );

        if ( !this.arguments.isEmpty() )
        {
            query.setParameters( this.arguments.toArray() );
        }

        if ( !this.sqlStatesToRetry.isEmpty() )
        {
            for ( String sqlState : sqlStatesToRetry )
            {
                query = query.retryOnSqlState( sqlState );
            }
        }

        if ( this.maxRows > 0 )
        {
            query.setMaxRows( this.maxRows );
        }

        return query;
    }
}

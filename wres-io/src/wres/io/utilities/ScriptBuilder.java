package wres.io.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
    private Integer parameterCount = 0;
    private Map<Integer, Class> parameterSpec;

    public ScriptBuilder()
    {
        this.script = new StringBuilder(  );
    }

    public ScriptBuilder (String beginning)
    {
        this.script = new StringBuilder( beginning );
    }

    public ScriptBuilder add(Object... details)
    {
        for (Object detail : details)
        {
            this.script.append(detail);
        }

        return this;
    }

    public ScriptBuilder addLine()
    {
        return this.add(NEWLINE);
    }

    public ScriptBuilder addLine(Object... details)
    {
        return this.add(details).addLine();
    }

    public ScriptBuilder addTab(int numberOfTabs)
    {
        for ( int i = 0; i < numberOfTabs; i++ )
        {
            this.add("    ");
        }

        return this;
    }

    public ScriptBuilder addTab()
    {
        return addTab(1);
    }

    @Override
    public String toString()
    {
        return this.script.toString();
    }

    public void execute(Object... parameters) throws SQLException
    {
        List<Object[]> parameterList = new ArrayList<>(  );
        parameterList.add( parameters );

        this.execute(parameterList );
    }

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

    public Future issue()
    {
        SQLExecutor executor = new SQLExecutor( this.toString() );
        return Database.execute( executor );
    }

    public Future issue(Object... parameters)
    {
        List<Object[]> parameterList = new ArrayList<>(  );
        parameterList.add( parameters );

        return this.issue(parameterList);
    }

    public Future issue(List<Object[]> parameters)
    {
        StatementRunner runner = new StatementRunner( this.toString(), parameters );
        return Database.execute( runner );
    }

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

    public <V> Future<V> submit(String label)
    {
        ValueRetriever<V> retriever = new ValueRetriever<V>( this.toString(), label );
        return Database.submit( retriever );
    }

    public PreparedStatement getPreparedStatement(final Connection connection)
            throws SQLException
    {
        return connection.prepareStatement( this.toString() );
    }

    public PreparedStatement getPreparedStatement(final Connection connection, Object... parameters)
            throws SQLException
    {
        List<Object[]> parameterList = new ArrayList<>(  );
        parameterList.add( parameters );

        return this.getPreparedStatement( connection, parameterList );
    }

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

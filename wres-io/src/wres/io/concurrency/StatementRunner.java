package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.utilities.Database;
import wres.util.Strings;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StatementRunner extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatementRunner.class);

    private final String script;
    private final List<Object[]> values;

    public StatementRunner (String script, List<Object[]> values)
    {

        if(values.size() == 0)
        {
            throw new IllegalArgumentException("There are no values to run for this statement.");
        }

        this.script = script;
        this.values = values;
    }

    @Override
    protected void execute ()
    {
        Connection connection = null;
        PreparedStatement statement = null;

        try
        {
            connection = Database.getConnection();
            statement = connection.prepareStatement(this.script);

            for (Object[] statementValues : this.values)
            {
                int addedParameters = 0;
                for (; addedParameters < statementValues.length; ++addedParameters)
                {
                    statement.setObject(addedParameters + 1, statementValues[addedParameters]);
                }

                while (addedParameters < statement.getParameterMetaData().getParameterCount())
                {
                    statement.setObject(addedParameters + 1, null);
                }

                statement.addBatch();
            }

            statement.executeBatch();
        }
        catch (SQLException e)
        {
            LOGGER.error("Error occured while running prepared statement:");
            LOGGER.error(this.script);
            LOGGER.error(Strings.getStackTrace(e));
        }
        finally
        {
            if (statement != null)
            {
                try
                {
                    statement.close();
                }
                catch (SQLException e)
                {
                    LOGGER.error("Could not close the prepared statement.");
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }
    }

    @Override
    protected Logger getLogger () {
        return LOGGER;
    }
}

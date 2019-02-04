package wres.system;

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;

public class DatabaseSchema
{
    public DatabaseSchema(final String databaseName)
    {
        this.databaseName = databaseName;
    }

    public void createDatabase(final Connection connection) throws SQLException
    {
        boolean databaseExists = false;
        boolean canAddDatabase = false;

        try (
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery( "SELECT * FROM pg_database;" )
        )
        {
            while ( results.next() )
            {
                String name = results.getString( 1 );
                if ( name.equalsIgnoreCase( this.databaseName ) )
                {
                    databaseExists = true;
                    break;
                }
            }
        }

        if (!databaseExists)
        {
            // TODO: If we support another database, we'll need to modify this to handle the others as well
            try (
                    Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery(
                            "SELECT rolcreatedb FROM pg_roles WHERE rolname = CURRENT_USER;"
                    )
            )
            {

                while ( results.next() )
                {
                    canAddDatabase = results.getBoolean( 1 );
                }

                if ( !canAddDatabase )
                {
                    throw new SQLException( "The database '" + this.databaseName + "' does not exist on '" +
                                            connection.getMetaData().getURL() + "' and you do not have "
                                            + "permission to create it. Please contact an "
                                            + "administrator to add it." );
                }

                String script = "CREATE DATABASE " + this.databaseName + ";";
                statement.execute(script);
            }
        }
    }

    // Left public for unit testing
    public String getChangelogURL()
    {
        URL changelogURL = this.getClass().getClassLoader().getResource( "database/db.changelog-master.xml" );
        Objects.requireNonNull( changelogURL, "The definition for the WRES data model could not be found.");
        return changelogURL.getPath();
    }

    public void applySchema(final Connection connection) throws SQLException, IOException
    {
        this.removePriorLocks( connection );
        Database database = null;
        try
        {
            database = DatabaseFactory.getInstance()
                                      .findCorrectDatabaseImplementation(
                                                       new JdbcConnection( connection )
                                               );
        }
        catch ( DatabaseException e )
        {
            throw new IOException("A database instance could not be accessed.");
        }

        try
        {
            Liquibase liquibase = new Liquibase(
                    this.getChangelogURL(),
                    new FileSystemResourceAccessor(  ),
                    database
            );

            Contexts contexts = new Contexts();
            LabelExpression expression = new LabelExpression();
            liquibase.update( contexts, expression );
        }
        catch (LiquibaseException e)
        {
            throw new SQLException( "The WRES could not be properly initialized.", e);
        }

        this.moveSchema( connection, "partitions", "wres" );

        // Allow other users to apply liquibase changes...
        try (Statement statement = connection.createStatement())
        {
            statement.execute( "ALTER TABLE public.databasechangelog OWNER TO wres;" );
            connection.commit();
        }

        try (Statement statement = connection.createStatement())
        {
            statement.execute( "ALTER TABLE public.databasechangeloglock OWNER TO wres;" );
            connection.commit();
        }
    }

    private void removePriorLocks(final Connection connection) throws SQLException, IOException
    {
        // Determine whether or not the changeloglock exists in the database
        String script = "SELECT EXISTS (" + System.lineSeparator();
        script += "    SELECT 1" + System.lineSeparator();
        script += "    FROM information_schema.tables" + System.lineSeparator();
        script += "    WHERE table_catalog = '" + databaseName + "'" + System.lineSeparator();
        script += "        AND table_name = 'databasechangeloglock'" + System.lineSeparator();
        script += ");";

        // Collect the address to every interface for the system. Liquibase keeps track of
        // lock ownership by the address of different lock interfaces
        ArrayList<String> addresses = new ArrayList<>();

        try (
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery( script )
        )
        {

            result.next();

            // Get the result from the database to determine whether or not it exists
            boolean changeLogLockExists = result.getBoolean( 1 );

            // If the change log lock table exists
            if ( changeLogLockExists )
            {

                try
                {
                    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                    for ( NetworkInterface networkInterface : Collections.list( interfaces ) )
                    {
                        for ( InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses() )
                        {
                            String address = interfaceAddress.getAddress().getHostAddress();
                            addresses.add( "%(" + address + ")%" );
                        }
                    }
                }
                catch ( SocketException e )
                {
                    throw new IOException( "Could not determine if this system already holds "
                                           + "liquibase locks due to I/O miscommunication.", e );
                }
            }
        }

        // If at least one address was determined...
        if ( !addresses.isEmpty() )
        {
            try (Statement statement = connection.createStatement())
            {
                // Forcibly remove any prior lock for this system.  Since we know that liquibase
                // stores ownership via address, we want to delete any locks that are associated
                // with this system, since any possible lock for this system will prevent this
                // system (the one that supposedly already owns the lock) from doing its work.
                StringJoiner builder = new StringJoiner(
                        ",",
                        "DELETE FROM databasechangeloglock WHERE lockedby LIKE ANY('{",
                        "}');"
                );
                addresses.forEach( builder::add );
                statement.execute( builder.toString() );
            }
        }
    }

    private void moveSchema(final Connection connection, final String from, final String to) throws SQLException
    {
        final String NEWLINE = System.lineSeparator();
        boolean schemaExists;

        try (Statement schemaExistence = connection.createStatement())
        {
            String script = "SELECT EXISTS (" + NEWLINE +
                            "    SELECT 1" + NEWLINE +
                            "    FROM information_schema.schemata" + NEWLINE +
                            "    WHERE schema_name = '" + from + "'" + NEWLINE +
                            ");";

            try (ResultSet existence = schemaExistence.executeQuery( script ))
            {
                existence.next();
                schemaExists = existence.getBoolean( "exists" );
            }
        }
        catch (SQLException e)
        {
            throw new SQLException( "While moving tables from the '" +
                                    from + "' schema to the '" + to +
                                    "' schema, it could not be determined if the schema '" +
                                    from + "' even exists.", e );
        }

        if (!schemaExists)
        {
            return;
        }

        List<String> originalTables = new ArrayList<>(  );

        try (Statement tableSelect = connection.createStatement())
        {
            String script = "SELECT table_schema || '.' || table_name AS table_name" + NEWLINE +
                            "FROM information_schema.tables" + NEWLINE +
                            "WHERE table_schema = '" + from + "';";

            try (ResultSet tables = tableSelect.executeQuery( script ))
            {
                while (tables.next())
                {
                    originalTables.add( tables.getString("table_name") );
                }
            }
        }
        catch (SQLException e)
        {
            throw new SQLException( "While moving tables from the '" + from +
                                    "' schema to the '" + to + "' schema, tables from the '" +
                                    from + "' schema could not be requested.", e );
        }

        try (Statement tableSelect = connection.createStatement())
        {
            String script = "SELECT '" + from + "' || '.' || table_name AS table_name" + NEWLINE +
                            "FROM information_schema.tables" + NEWLINE +
                            "WHERE table_schema = '" + to + "'";

            try (ResultSet tables = tableSelect.executeQuery( script ))
            {
                // Remove any tables from the list tables that will get moved if there would be a conflict.
                while (tables.next())
                {
                    String preexistingTable = tables.getString( "table_name" );
                    originalTables.remove(preexistingTable);

                    if (originalTables.isEmpty())
                    {
                        break;
                    }
                }
            }
        }

        if (!originalTables.isEmpty())
        {
            String table = null;

            try (Statement schemaChange = connection.createStatement())
            {
                for (String tableName : originalTables)
                {
                    table = tableName;
                    schemaChange.execute( "ALTER TABLE " + table + " SET SCHEMA " + to + ";" );
                }
                connection.commit();
            }
            catch ( SQLException e )
            {
                connection.rollback();

                if (table == null)
                {
                    throw new SQLException(originalTables.size() + " tables from the '" +
                                           from + "' schema could not be moved to the '" +
                                           to + "' schema.", e);
                }
                else
                {
                    throw new SQLException( "The table named '" + table +
                                            "' could not be moved from the '" + from +
                                            "' schema to the '" + to + "' schema.", e  );
                }
            }
        }
    }

    private final String databaseName;
}

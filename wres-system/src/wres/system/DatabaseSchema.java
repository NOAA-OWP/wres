package wres.system;

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
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
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseSchema
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseSchema.class );

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

                statement.execute("CREATE DATABASE " + this.databaseName + ";"  );
            }
        }
    }

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

            // Liquibase sends a lot of information to its own internal logging system that spits everything out to
            // stdout at the 'info' level. Changing it to 'severe' (i.e. error) to prevent all of the diagnostic
            // messaging.
            liquibase.getLog().setLogLevel( "severe" );

            Contexts contexts = new Contexts();
            LabelExpression expression = new LabelExpression();
            liquibase.update( contexts, expression );
        }
        catch (LiquibaseException e)
        {
            throw new SQLException( "The WRES could not be properly initialized.", e);
        }

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

    private final String databaseName;
}

package wres.io.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


/**
 * Used as a utility class to get credentials from a PgPass file.
 * The idea is to not have any instances lying around with credentials in them.
 * Does not support the environment variables PGPASSFILE nor PGPASSWORD, always
 * looks in user's home dir for .pgpass
 */

public class PgPassReader
{
    private static final Path PG_PASS_PATH = Paths.get( System.getProperty( "user.home" ), ".pgpass" );

    private PgPassReader()
    {
        // Static helper class, no construction
    }

    static boolean pgPassExists()
    {
        File pgPassFile = PG_PASS_PATH.toFile();
        return pgPassFile.exists();
    }

    static boolean pgPassReadable()
    {
        File pgPassFile = PG_PASS_PATH.toFile();
        return pgPassFile.canRead();
    }

    static boolean pgPassExistsAndReadable()
    {
        return PgPassReader.pgPassExists()
               && PgPassReader.pgPassReadable();
    }

    /**
     * Attempt to get a passphrase from .pgpass file in user's home directory.
     * Should call pgPassExistsAndReadable() before calling, otherwise an
     * IOException will be thrown when the file is not found or not readable.
     * @param hostname the db hostname to search for (literally, exactly)
     * @param port the db port to search for, usually 5432
     * @param databaseName the database name to search for
     * @param username the db username to search for
     * @return the passphrase or null if combination not found
     * @throws IOException when something goes wrong reading the file
     */

    static String getPassphrase( String hostname, int port, String databaseName, String username )
            throws IOException
    {
        List<String> pgPassLines = Files.readAllLines( PG_PASS_PATH );

        for ( String line : pgPassLines )
        {
            String trimmed = line.trim();

            // Skip commented lines
            if ( trimmed.startsWith( "#" ) )
            {
                continue;
            }

            String[] splitted = trimmed.split( ":" );

            // Valid lines are 5 elements in length
            if ( splitted.length != 5 )
            {
                continue;
            }

            int specifiedPort;

            // Valid ports are able to be integers
            try
            {
                specifiedPort = Integer.parseInt( splitted[1] );
            }
            catch ( NumberFormatException nfe )
            {
                continue;
            }

            if ( splitted[0].equals( hostname )
                 && specifiedPort == port
                 && splitted[2].equals( databaseName )
                 && splitted[3].equals( username ) )
            {
                return splitted[4];
            }
        }

        // Nothing found.
        return null;
    }
}

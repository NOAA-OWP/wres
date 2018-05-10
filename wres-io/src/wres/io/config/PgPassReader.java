package wres.io.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;


/**
 * Used as a utility class to get credentials from a PgPass file.
 * The idea is to not have any instances lying around with credentials in them.
 * Supports environment variable PGPASSFILE but not PGPASSWORD, supports
 * Windows and Unix conventions of where to find pgpass according to
 * https://www.postgresql.org/docs/9.6/static/libpq-pgpass.html
 *
 * Disagreements with the description at above url:
 * Does not check permissions on the file other than this process having read
 * access. Interprets the empty string as "localhost" which may not be exactly
 * correct.
 */

public class PgPassReader
{
    private static final Path PG_PASS_PATH = PgPassReader.getPgPassPath();
    private static final String MATCH_ANY = "*";

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

            if ( PgPassReader.hostnameMatches( splitted[0], hostname )
                 && PgPassReader.portMatches( splitted[1], port )
                 && PgPassReader.stringMatches( splitted[2], databaseName )
                 && PgPassReader.stringMatches( splitted[3], username ) )
            {
                return splitted[4];
            }
        }

        // Nothing found.
        return null;
    }


    private static boolean hostnameMatches( String inPgPass, String specified )
    {
        Objects.requireNonNull( inPgPass );
        Objects.requireNonNull( specified );

        // Special case:
        // "Each of the first four fields can ... *, which matches anything"
        if ( inPgPass.equals( MATCH_ANY ) )
        {
            return true;
        }

        // Special case: empty matches localhost, see #45798-13
        // Not sure if this is actually how it is supposed to work...
        if ( inPgPass.isEmpty() && specified.equals( "localhost" ) )
        {
            return true;
        }

        return inPgPass.equals( specified );
    }


    private static boolean portMatches( String inPgPass, int specified )
    {
        Objects.requireNonNull( inPgPass );

        // Special case:
        // "Each of the first four fields can ... *, which matches anything"
        if ( inPgPass.equals( MATCH_ANY ) )
        {
            return true;
        }

        int parsedFromPgPass;

        // Valid ports are able to be integers
        try
        {
            parsedFromPgPass = Integer.parseInt( inPgPass );
        }
        catch ( NumberFormatException nfe )
        {
            return false;
        }

        return parsedFromPgPass == specified;
    }

    private static boolean stringMatches( String inPgPass, String specified )
    {
        Objects.requireNonNull( inPgPass );
        Objects.requireNonNull( specified );

        // Special case:
        // "Each of the first four fields can ... *, which matches anything"
        if ( inPgPass.equals( MATCH_ANY ) )
        {
            return true;
        }

        return inPgPass.equals( specified );
    }

    private static Path getPgPassPath()
    {
        // If $PGPASSFILE exists, it overrides all.
        if ( System.getenv( "PGPASSFILE" ) != null )
        {
            return Paths.get( System.getenv( "PGPASSFILE" ) );
        }

        if ( System.getProperty( "os.name" ) != null
             && System.getProperty( "os.name" )
                      .toLowerCase()
                      .contains( "windows" ) )
        {
            // When on windows, %APPDATA%\postgres\pgpass.conf
            return Paths.get( System.getenv( "APPDATA" ),
                              "postgres",
                              "pgpass.conf" );
        }
        else
        {
            // When on unix, $HOME/.pgpass
            return Paths.get( System.getProperty( "user.home" ),
                              ".pgpass" );
        }
    }
}

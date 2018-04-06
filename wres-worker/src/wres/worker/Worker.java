package wres.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Worker.class );

    /**
     * Expects exactly one arg with a path to WRES executable
     * @param args arguments, but only one is expected, a WRES executable
     * @throws IOException when process start fails.
     * @throws IllegalArgumentException when the first argument is not a WRES executable
     */

    public static void main( String[] args ) throws IOException
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres path." );
        }

        // Getting as a file allows us to verify it exists
        File wresExecutable = Paths.get( args[0] ).toFile();
        if ( !wresExecutable.exists() )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres *path*." );
        }
        else if ( !wresExecutable.canExecute() )
        {
            throw new IllegalArgumentException( "First arg must be an *executable* wres path." );
        }

        String command = "execute";
        String projectFile = "project_config.xml";

        ProcessBuilder processBuilder = new ProcessBuilder( wresExecutable.getPath(),
                                                            command,
                                                            projectFile );

        // Cause process builder to echo the subprocess's output when started.
        processBuilder.inheritIO();

        Process process = processBuilder.start();

        LOGGER.info( "Started subprocess {}", process );

        try
        {
            process.waitFor();
            int exitValue = process.exitValue();
            LOGGER.info( "Subprocess {} exited {}", process, exitValue );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while waiting for {}.", process );
            Thread.currentThread().interrupt();
        }
    }
}

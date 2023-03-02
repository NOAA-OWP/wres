package wres.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Used to download the object at the indicated path to the indicated target
 */
public final class Downloader implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Downloader.class );
    private static final Object DIRECTORY_CREATION_LOCK = new Object();

    /**
     * Creates an instance.
     * @param targetPath the target path
     * @param address the address
     */
    public Downloader( Path targetPath, URI address )
    {
        this.targetPath = targetPath;
        this.address = address;
    }

    /**
     * Sets the output display state.
     * @param displayOutput is true to display output, false to hide
     */
    public void setDisplayOutput( boolean displayOutput )
    {
        this.displayOutput = displayOutput;
    }

    @Override
    public void run()
    {
        String message = this.address + "\t|\t";

        try
        {
            final URL fileURL = address.toURL();
            HttpURLConnection connection = ( HttpURLConnection ) fileURL.openConnection();

            if ( connection.getResponseCode() >= 400 )
            {
                message += "Cannot be accessed\t|\t";
            }
            else
            {
                message += "Exists\t\t\t|\t";
                message += this.copy( fileURL );
            }

        }
        catch ( IOException e )
        {
            String errorMessage = "While attempting to download a file at "
                                  + this.address
                                  + ", encountered an error.";
            LOGGER.error( errorMessage, e );
        }

        if ( displayOutput )
        {
            LOGGER.info( message );
        }
    }

    private String copy( URL url )
    {
        try ( InputStream fileStream = url.openStream() )
        {
            synchronized ( DIRECTORY_CREATION_LOCK )
            {
                if ( Files.notExists( this.targetPath.getParent() ) )
                {
                    Files.createDirectories( this.targetPath.getParent() );
                }
            }

            Files.copy( fileStream, this.targetPath, StandardCopyOption.REPLACE_EXISTING );
            this.fileDownloaded = true;

            return "Downloaded";
        }
        catch ( IOException saveError )
        {
            String errorMessage = "While attempting to download a file at "
                                  + this.address
                                  + " to "
                                  + this.targetPath
                                  + ", encountered an error.";
            LOGGER.error( errorMessage, saveError );
            return "Not downloaded";
        }
    }

    /**
     * @return true if the file has been downloaded.
     */
    public boolean fileHasBeenDownloaded()
    {
        return this.fileDownloaded;
    }

    private final Path targetPath;
    private final URI address;
    private boolean fileDownloaded = false;
    private boolean displayOutput;
}

package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.util.Strings;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Thread used to download the object at the indicated path to the indicated target
 */
public final class Downloader extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);
    private static final Object DIRECTORY_CREATION_LOCK = new Object();

    public Downloader( Path targetPath, URI address )
    {
        this.targetPath = targetPath;
        this.address = address;
    }

    public void setDisplayOutput(boolean displayOutput)
    {
        this.displayOutput = displayOutput;
    }

    @Override
    public void execute() {
        String message = this.address + "\t|\t";

        try
        {
            final URL fileURL = address.toURL();
            HttpURLConnection connection = (HttpURLConnection) fileURL.openConnection();

            if (connection.getResponseCode() >= 400)
            {
                message += "Cannot be accessed\t|\t";
            }
            else
            {
                message += "Exists\t\t\t|\t";

                message += copy( fileURL );
            }

        }
        catch (java.io.IOException e)
        {
            this.getLogger().error("The address: '{}' is not a valid url.{}",
                                   this.address,
                                   NEWLINE);

            this.getLogger().error(Strings.getStackTrace(e));
        }

        if (displayOutput)
        {
            this.getLogger().info(message);
        }

        this.executeOnComplete();
    }

    private String copy(URL url)
    {
        try (InputStream fileStream = url.openStream())
        {
            synchronized ( DIRECTORY_CREATION_LOCK )
            {
                if ( Files.notExists( this.targetPath.getParent() ) )
                {
                    Files.createDirectories( this.targetPath.getParent() );
                }
            }

            Files.copy(fileStream, this.targetPath, StandardCopyOption.REPLACE_EXISTING);
            this.fileDownloaded = true;

            return "Downloaded";
        }
        catch (java.io.IOException saveError)
        {
            this.getLogger().error("{}The file at '{}' could not be saved to:{} '{}'.",
                                   NEWLINE,
                                   this.address,
                                   NEWLINE,
                                   this.targetPath.toString());
            this.getLogger().error( Strings.getStackTrace( saveError ) );
            return "Not Downloaded";
        }
    }

    public boolean fileHasBeenDownloaded()
    {
        return this.fileDownloaded;
    }

    private final Path targetPath;
    private final URI address;
    private boolean fileDownloaded = false;
    private boolean displayOutput;

    @Override
    protected Logger getLogger () {
        return Downloader.LOGGER;
    }
}

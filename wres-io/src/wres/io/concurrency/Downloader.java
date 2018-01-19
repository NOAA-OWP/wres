package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.util.Strings;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Thread used to download the object at the indicated path to the indicated target
 */
public final class Downloader extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);
    private boolean displayOutput;

    public Downloader(Path targetPath, String address)
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
            final URL fileURL = new URL(address);
            HttpURLConnection connection = (HttpURLConnection) fileURL.openConnection();

            if (displayOutput && connection.getResponseCode() >= 400)
            {
                message += "Cannot be accessed\t|\t";
            }
            else if (displayOutput)
            {
                message += "Exists\t\t\t|\t";
            }

            message += copy( fileURL );

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
            Files.copy(fileStream, this.targetPath, StandardCopyOption.REPLACE_EXISTING);

            return "Downloaded";
        }
        catch (java.io.IOException saveError)
        {
            this.getLogger().error("{}The file at '{}' could not be saved to:{} '{}'.",
                                   NEWLINE,
                                   this.address,
                                   NEWLINE,
                                   this.targetPath.toString());
            return "Not Downloaded";
        }
    }

    private final Path targetPath;
    private final String address;

    @Override
    protected Logger getLogger () {
        return Downloader.LOGGER;
    }
}

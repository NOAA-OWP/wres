package concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.concurrency.WRESRunnable;

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

    public Downloader(Path targetPath, String address)
    {
        this.targetPath = targetPath;
        this.address = address;
    }

    @Override
    public void execute() {
        String message = this.address + "\t|\t";

        try {
            final URL fileURL = new URL(address);
            HttpURLConnection connection = (HttpURLConnection) fileURL.openConnection();

            if (connection.getResponseCode() >= 400)
            {
                message += "Cannot be accessed\t|\t";
            }
            else
            {
                message += "Exists\t\t\t|\t";
            }

            try (InputStream fileStream = fileURL.openStream())
            {
                Files.copy(fileStream, this.targetPath, StandardCopyOption.REPLACE_EXISTING);

                message += "Downloaded";
            }
            catch (java.io.IOException saveError)
            {
                this.getLogger().trace("The file at '" + this.address + "' could not be saved to: '" + this.targetPath.toString() + "'.");
                message += "Not Downloaded";
            }

        } catch (java.io.IOException e) {
            this.getLogger().error("The address: '" + this.address + "' is not a valid url.");
            e.printStackTrace();
        }

        this.getLogger().info(message);
        this.executeOnComplete();
    }

    private final Path targetPath;
    private final String address;

    @Override
    protected String getTaskName () {
        return "Downloader: " + this.address;
    }

    @Override
    protected Logger getLogger () {
        return Downloader.LOGGER;
    }
}

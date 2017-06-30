package concurrency;

import wres.io.concurrency.WRESTask;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Thread used to download the object at the indicated path to the indicated target
 */
public final class Downloader extends WRESTask implements Runnable {

    public Downloader(Path targetPath, String address)
    {
        this.targetPath = targetPath;
        this.address = address;
    }

    @Override
    public void run() {
        this.executeOnRun();
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
                System.err.println("The file at '" + this.address + "' could not be saved to: '" + this.targetPath.toString() + "'.");
                message += "Not Downloaded";
            }

        } catch (java.io.IOException e) {
            System.err.println("The address: '" + this.address + "' is not a valid url.");
            e.printStackTrace();
        }

        System.out.println(message);
        this.executeOnComplete();
    }

    private final Path targetPath;
    private final String address;
}

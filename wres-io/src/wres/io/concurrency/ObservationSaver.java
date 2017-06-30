package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.reading.BasicSource;
import wres.io.reading.ReaderFactory;
import wres.util.FormattedStopwatch;

import java.io.IOException;

/**
 * Saves the observation at the given location
 * 
 * @author Christopher Tubbs
 */
public class ObservationSaver extends WRESTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationSaver.class);
    
	/**
	 * Creates a new save with the given path
	 */
	public ObservationSaver(String filepath) {
		this.filepath = filepath;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public void run() {
		BasicSource source;
		this.executeOnRun();
		try {
			source = ReaderFactory.getReader(this.filepath);
			FormattedStopwatch watch = new FormattedStopwatch();
            if (LOGGER.isDebugEnabled())
            {
                watch.start();
                LOGGER.debug("Attempting to save '" + this.filepath +"' to the database...");
            }
			source.saveObservation();

            if (LOGGER.isDebugEnabled())
            {
                watch.stop();
                LOGGER.debug("'" + this.filepath+ "' attempt to save to the database took "
                                     + watch.getFormattedDuration());
            }
		}
        catch (IOException ioe)
        {
            LOGGER.error("Failed to save '{}' as an observation", filepath);
            LOGGER.error("The exception:", ioe);
        }
		this.executeOnComplete();
	}

	private String filepath = null;
}

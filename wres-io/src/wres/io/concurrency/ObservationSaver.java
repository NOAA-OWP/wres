package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.reading.BasicSource;
import wres.io.reading.SourceReader;
import wres.io.utilities.Debug;
import wres.util.FormattedStopwatch;

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
			source = SourceReader.get_source(this.filepath);
			FormattedStopwatch watch = new FormattedStopwatch();
			watch.start();
			Debug.debug(LOGGER, "Attempting to save '%s' to the database...", System.out, this.filepath);
			source.save_observation();
			watch.stop();
			Debug.debug(LOGGER, "'" + this.filepath + "' has been saved to the database after " + watch.getFormattedDuration(), System.out);
		} catch (Exception e) {
			System.err.println("Failed to save '" + String.valueOf(filepath) + " as an observation.");
			e.printStackTrace();
		}
		this.exectureOnComplete();
	}

	private String filepath = null;
}

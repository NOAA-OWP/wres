/**
 * 
 */
package concurrency;

import reading.BasicSource;
import reading.SourceReader;
import util.Stopwatch;

/**
 * Saves the observation at the given location
 */
public class ObservationSaver implements Runnable {

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
	/**
	 * Attempts to save the file at the given location to the database as an observation
	 */
	public void run() {
		BasicSource source;
		try {
			source = SourceReader.get_source(this.filepath);
			Stopwatch watch = new Stopwatch();
			watch.start();
			System.out.println(String.format("Attempting to save '%s' to the database...", this.filepath));
			source.save_observation();
			watch.stop();
			System.out.println("'" + this.filepath + "' has been saved to the database after " + watch.get_formatted_duration());
		} catch (Exception e) {
			System.err.println("Failed to save '" + String.valueOf(filepath) + " as an observation.");
			e.printStackTrace();
		}
	}

	private String filepath = null;
}

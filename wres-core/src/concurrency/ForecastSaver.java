/**
 * 
 */
package concurrency;

import reading.BasicSource;
import reading.SourceReader;

/**
 * Saves the forecast at the indicated path asynchronously
 */
public class ForecastSaver implements Runnable {

	/**
	 * Creates the saver with the given path to a file containing observation data
	 */
	public ForecastSaver(String filepath) 
	{
		this.filepath = filepath;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	/**
	 * Attempts to save data in the given file as a forecast in the given location
	 */
	public void run() {
		try
		{
			BasicSource source = SourceReader.get_source(this.filepath);
			source.save_forecast();
			System.out.println(this.filepath + " saved to the database as a forecast. Please verify data.");
		}
		catch (Exception e)
		{
			System.err.println("A forecast for the data at '" + this.filepath + "' could not be saved to the database.");
			e.printStackTrace();
		}

	}

	private String filepath = null;
}

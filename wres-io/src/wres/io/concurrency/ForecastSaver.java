package wres.io.concurrency;

import wres.io.reading.BasicSource;
import wres.io.reading.SourceReader;

/**
 * Saves the forecast at the indicated path asynchronously
 * 
 * @author Christopher Tubbs
 */
public class ForecastSaver extends WRESThread implements Runnable {

	/**
	 * Creates the saver with the given path to a file containing observation data
	 * @param filepath The path to the file to save as a forecast
	 */
    public ForecastSaver(String filepath) {
        this.filepath = filepath;
    }

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public void run() {
	    this.executeOnRun();
		try
		{
			BasicSource source = SourceReader.get_source(this.filepath);
			source.save_forecast();
		}
		catch (Exception e)
		{
			System.err.println("A forecast for the data at '" + this.filepath + "' could not be saved to the database.");
			e.printStackTrace();
		}
		
		this.exectureOnComplete();
	}

	private String filepath = null;
}

package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.reading.BasicSource;
import wres.io.reading.ReaderFactory;
import wres.util.Strings;

/**
 * Saves the forecast at the indicated path asynchronously
 * 
 * @author Christopher Tubbs
 */
public class ForecastSaver extends WRESRunnable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ForecastSaver.class);

	/**
	 * Creates the saver with the given path to a file containing observation data
	 * @param filepath The path to the file to save as a forecast
	 */
    public ForecastSaver(String filepath) {
        this.filepath = filepath;
        this.datasource = null;
    }

    public ForecastSaver(String filepath, ProjectDataSpecification datasource)
	{
		this.filepath = filepath;
		this.datasource = datasource;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public void execute() {
		try
		{
			BasicSource source = ReaderFactory.getReader(this.filepath);

			if (this.datasource != null)
			{
				source.applySpecification(datasource);
			}

			source.saveForecast();
		}
		catch (Exception e)
		{
			this.getLogger().error("A forecast for the data at '" + this.filepath + "' could not be saved to the database.");
            this.getLogger().error(Strings.getStackTrace(e));
		}
	}

	private String filepath = null;
	private final ProjectDataSpecification datasource;

	@Override
	protected String getTaskName () {
		return "ForecastSaver: " + this.filepath;
	}

    @Override
    protected Logger getLogger () {
        return ForecastSaver.LOGGER;
    }
}

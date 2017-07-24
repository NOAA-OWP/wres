package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.io.reading.BasicSource;
import wres.io.reading.ReaderFactory;
import wres.util.FormattedStopwatch;

import java.io.IOException;
import java.util.List;

/**
 * Saves the observation at the given location
 * 
 * @author Christopher Tubbs
 */
public class ObservationSaver extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationSaver.class);
    
	/**
	 * Creates a new save with the given path
	 */
	public ObservationSaver(String filepath) {
		this.filepath = filepath;
		this.dataSourceConfig = null;
		this.specifiedFeatures = null;
	}

	public ObservationSaver(String filepath,
                            DataSourceConfig dataSourceConfig,
                            List<Conditions.Feature> specifiedFeatures)
    {
        this.dataSourceConfig = dataSourceConfig;
        this.specifiedFeatures = specifiedFeatures;
        this.filepath = filepath;
    }

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public void execute() {
		BasicSource source;

		try {
			source = ReaderFactory.getReader(this.filepath);
			FormattedStopwatch watch = new FormattedStopwatch();
            if (this.getLogger().isDebugEnabled())
            {
                watch.start();
				this.getLogger().debug("Attempting to save '" + this.filepath +"' to the database...");
            }

            if (this.dataSourceConfig != null)
            {
                source.setDataSourceConfig(this.dataSourceConfig);
            }

            if (this.specifiedFeatures != null)
            {
                source.setSpecifiedFeatures(this.specifiedFeatures);
            }

			source.saveObservation();

            if (this.getLogger().isDebugEnabled())
            {
                watch.stop();
				this.getLogger().debug("'" + this.filepath+ "' attempt to save to the database took "
                                     + watch.getFormattedDuration());
            }
		}
        catch (IOException ioe)
        {
			this.getLogger().error("Failed to save '{}' as an observation", filepath);
			this.getLogger().error("The exception:", ioe);
        }
	}

	private String filepath = null;
	private final List<Conditions.Feature> specifiedFeatures;
	private final DataSourceConfig dataSourceConfig;

	@Override
	protected String getTaskName () {
		return "ObservationSaver: " + this.filepath;
	}

	@Override
	protected Logger getLogger () {
		return ObservationSaver.LOGGER;
	}
}

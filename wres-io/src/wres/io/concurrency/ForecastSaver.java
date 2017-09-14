package wres.io.concurrency;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.ReaderFactory;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Saves the forecast at the indicated path asynchronously
 * 
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class ForecastSaver extends WRESRunnable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ForecastSaver.class);

    @Internal(exclusivePackage = "wres.io")
	public ForecastSaver(String filepath,
						 ProjectDetails projectDetails,
						 DataSourceConfig dataSourceConfig,
						 List<Feature> specifiedFeatures)
    {
        this.dataSourceConfig = dataSourceConfig;
        this.filepath = filepath;
        this.specifiedFeatures = specifiedFeatures;
        this.projectDetails = projectDetails;
    }

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public void execute() {
		try
		{
			BasicSource source = ReaderFactory.getReader(this.filepath);

			source.setDataSourceConfig(this.dataSourceConfig);

			source.setSpecifiedFeatures(this.specifiedFeatures);

			source.setProjectDetails( this.projectDetails );

			source.saveForecast();
		}
		catch (Exception e)
		{
			this.getLogger().error("A forecast for the data at '{}' could not be saved to the database.",
                                   this.filepath);
            this.getLogger().error(Strings.getStackTrace(e));
		}
	}

	private final String filepath;
	private final DataSourceConfig dataSourceConfig;
	private final List<Feature> specifiedFeatures;
	private final ProjectDetails projectDetails;

    @Override
    protected Logger getLogger () {
        return ForecastSaver.LOGGER;
    }
}

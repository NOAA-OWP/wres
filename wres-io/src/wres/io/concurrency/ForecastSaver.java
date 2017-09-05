package wres.io.concurrency;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
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
						 DataSourceConfig dataSourceConfig,
						 List<Conditions.Feature> specifiedFeatures)
    {
        this.dataSourceConfig = dataSourceConfig;
        this.filepath = filepath;
        this.specifiedFeatures = specifiedFeatures;
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

			source.saveForecast();
		}
		catch (Exception e)
		{
			this.getLogger().error("A forecast for the data at '" + this.filepath + "' could not be saved to the database.");
            this.getLogger().error(Strings.getStackTrace(e));
		}
	}

	private String filepath = null;
	private final DataSourceConfig dataSourceConfig;
	private final List<Conditions.Feature> specifiedFeatures;

    @Override
    protected Logger getLogger () {
        return ForecastSaver.LOGGER;
    }
}

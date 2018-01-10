package wres.io.concurrency;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.reading.ReaderFactory;

/**
 * Saves the forecast at the indicated path asynchronously
 *
 * @author Christopher Tubbs
 */
public class ForecastSaver extends WRESCallable<List<IngestResult>>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ForecastSaver.class);

	private final DataSourceConfig.Source sourceConfig;

    public ForecastSaver(String filepath,
                         ProjectConfig projectConfig,
						 DataSourceConfig dataSourceConfig,
						 DataSourceConfig.Source sourceConfig,
						 List<Feature> specifiedFeatures)
    {
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.filepath = filepath;
        this.specifiedFeatures = specifiedFeatures;
        this.projectConfig = projectConfig;
    }

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public List<IngestResult> execute()
    {
		try
		{
            BasicSource source = ReaderFactory.getReader( this.projectConfig,
                                                          this.filepath );

			source.setDataSourceConfig(this.dataSourceConfig);

			source.setSourceConfig( this.sourceConfig );

			source.setSpecifiedFeatures(this.specifiedFeatures);

            return source.saveForecast();
		}
        catch ( IOException e )
        {
            String message = "A forecast for the data at '"
                             + this.filepath
                             + "' could not be saved to the database.";
            throw new RuntimeException( message, e );
        }
	}

	private final String filepath;
	private final DataSourceConfig dataSourceConfig;
	private final List<Feature> specifiedFeatures;
    private final ProjectConfig projectConfig;

    @Override
    protected Logger getLogger () {
        return ForecastSaver.LOGGER;
    }
}

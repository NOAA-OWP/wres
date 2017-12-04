package wres.io.concurrency;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.ReaderFactory;
import wres.util.Internal;

/**
 * Saves the forecast at the indicated path asynchronously
 * 
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class ForecastSaver extends WRESCallable<List<String>>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ForecastSaver.class);

	private final DataSourceConfig.Source sourceConfig;

    @Internal(exclusivePackage = "wres.io")
	public ForecastSaver(String filepath,
						 ProjectDetails projectDetails,
						 DataSourceConfig dataSourceConfig,
						 DataSourceConfig.Source sourceConfig,
						 List<Feature> specifiedFeatures)
    {
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.filepath = filepath;
        this.specifiedFeatures = specifiedFeatures;
        this.projectDetails = projectDetails;
    }

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public List<String> execute()
    {
		try
		{
			BasicSource source = ReaderFactory.getReader(this.filepath);

			source.setDataSourceConfig(this.dataSourceConfig);

			source.setSourceConfig( this.sourceConfig );

			source.setSpecifiedFeatures(this.specifiedFeatures);

			source.setProjectDetails( this.projectDetails );

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
	private final ProjectDetails projectDetails;

    @Override
    protected Logger getLogger () {
        return ForecastSaver.LOGGER;
    }
}

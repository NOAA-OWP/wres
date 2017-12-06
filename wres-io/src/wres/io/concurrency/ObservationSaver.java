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
import wres.util.Internal;

/**
 * Saves the observation at the given location
 * 
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class ObservationSaver extends WRESCallable<List<IngestResult>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationSaver.class);

    private final DataSourceConfig.Source sourceConfig;

    @Internal(exclusivePackage = "wres.io")
	public ObservationSaver(String filepath,
                            ProjectConfig ProjectConfig,
                            DataSourceConfig dataSourceConfig,
							DataSourceConfig.Source sourceConfig,
                            List<Feature> specifiedFeatures)
    {
        this.dataSourceConfig = dataSourceConfig;
		this.sourceConfig = sourceConfig;
        this.projectConfig = ProjectConfig;
        this.specifiedFeatures = specifiedFeatures;
        this.filepath = filepath;
    }

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
    public List<IngestResult> execute()
	{
		BasicSource source;

		try {
            source = ReaderFactory.getReader( this.projectConfig,
                                              this.filepath );

            source.setDataSourceConfig(this.dataSourceConfig);

			source.setSourceConfig( this.sourceConfig );

            source.setSpecifiedFeatures( this.specifiedFeatures );

            return source.saveObservation();
		}
        catch (IOException ioe)
        {
            String message = "Failed to save '" + filepath
                             + "' as an observation";
            throw new RuntimeException( message, ioe );
        }
	}

	private final String filepath;
	private final List<Feature> specifiedFeatures;
	private final DataSourceConfig dataSourceConfig;
    private final ProjectConfig projectConfig;

	@Override
	protected Logger getLogger () {
		return ObservationSaver.LOGGER;
	}
}

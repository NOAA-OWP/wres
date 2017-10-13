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
 * Saves the observation at the given location
 * 
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class ObservationSaver extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationSaver.class);

    private final DataSourceConfig.Source sourceConfig;

    @Internal(exclusivePackage = "wres.io")
	public ObservationSaver(String filepath,
                            ProjectDetails projectDetails,
                            DataSourceConfig dataSourceConfig,
							DataSourceConfig.Source sourceConfig,
                            List<Feature> specifiedFeatures)
    {
        this.dataSourceConfig = dataSourceConfig;
		this.sourceConfig = sourceConfig;
        this.projectDetails = projectDetails;
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

            source.setDataSourceConfig(this.dataSourceConfig);

			source.setSourceConfig( this.sourceConfig );

            source.setSpecifiedFeatures( this.specifiedFeatures );

            source.setProjectDetails( this.projectDetails );
			source.saveObservation();
		}
        catch (IOException ioe)
        {
			this.getLogger().error("Failed to save '{}' as an observation", filepath);
			this.getLogger().error("The exception:", ioe);
        }
	}

	private final String filepath;
	private final List<Feature> specifiedFeatures;
	private final DataSourceConfig dataSourceConfig;
	private final ProjectDetails projectDetails;

	@Override
	protected Logger getLogger () {
		return ObservationSaver.LOGGER;
	}
}

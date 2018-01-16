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
 * Saves the observation at the given location
 * 
 * @author Christopher Tubbs
 */
public class ObservationSaver extends WRESCallable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ObservationSaver.class );

    private final String filepath;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
    private final DataSourceConfig.Source sourceConfig;
    private final List<Feature> specifiedFeatures;

    public ObservationSaver( String filepath,
                             ProjectConfig projectConfig,
                             DataSourceConfig dataSourceConfig,
                             DataSourceConfig.Source sourceConfig,
                             List<Feature> specifiedFeatures )
    {
        this.filepath = filepath;
        this.projectConfig = projectConfig;
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.specifiedFeatures = specifiedFeatures;
    }


    @Override
    public List<IngestResult> execute() throws IOException
    {
        BasicSource source = ReaderFactory.getReader( this.projectConfig,
                                                      this.filepath );
        source.setDataSourceConfig( this.dataSourceConfig );
        source.setSourceConfig( this.sourceConfig );
        source.setSpecifiedFeatures( this.specifiedFeatures );
        return source.saveObservation();
    }


    @Override
    protected Logger getLogger()
    {
        return ObservationSaver.LOGGER;
    }
}

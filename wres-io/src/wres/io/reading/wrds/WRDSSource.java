package wres.io.reading.wrds;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;

public class WRDSSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WRDSSource.class );

    public WRDSSource( ProjectConfig projectConfig, final String filename )
    {
        super( projectConfig );
        this.setFilename( filename );
    }

    @Override
    public List<IngestResult> save() throws IOException
    {
        if (!ConfigHelper.isForecast( this.getDataSourceConfig() ))
        {
            return this.saveObservation();
        }

        URL dataPath;

        if (this.getIsRemote())
        {
            dataPath = new URL( this.getFilename() );
        }
        else
        {
            dataPath = Paths.get(this.getFilename()).toUri().toURL();
        }

        ForecastReader reader = new ForecastReader( this.getProjectConfig(),
                                                    this.getDataSourceConfig(),
                                                    dataPath);

        return reader.save();
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        throw new IngestException( "WRDS does not currently support observations." );
    }

    @Override
    protected Logger getLogger()
    {
        return WRDSSource.LOGGER;
    }
}

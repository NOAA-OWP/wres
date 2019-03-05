package wres.io.reading.wrds;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;

/**
 * Ingests JSON data from the WRDS schema from either the service or JSON files on the file system
 * <p>
 *     As of 11/23/2018, the url for the swagger document is: http://***REMOVED***.***REMOVED***.***REMOVED***/api/v1/20181119/docs
 *     The project config will be needed to be modified to let the user indicate the format + the
 *     service where the data will come from. Currently, if you try to get the data from the service,
 *     it will attempt to find the service on the file system in the sourceloader and fail.
 * </p>
 * <p>
 *     Additional logic will be needed to fill service parameters, such as issued time ranges,
 *     valid time ranges, and location IDs.
 * </p>
 */
public class WRDSSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WRDSSource.class );

    public WRDSSource( ProjectConfig projectConfig, final URI filename )
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

        ReadValueManager reader = new ReadValueManager( this.getProjectConfig(),
                                                        this.getDataSourceConfig(),
                                                        this.getFilename() );

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

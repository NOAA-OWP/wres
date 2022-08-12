package wres.io.reading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.Caches;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.commaseparated.CSVSource;
import wres.io.reading.datacard.DatacardSource;
import wres.io.reading.fews.FEWSSource;
import wres.io.reading.nwm.GriddedNWMSource;
import wres.io.reading.waterml.WaterMLBasicSource;
import wres.io.reading.wrds.WRDSSource;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * @author ctubbs
 * @author James Brown
 */
public class ReaderFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ReaderFactory.class );

    public static Source getReader( TimeSeriesIngester timeSeriesIngester,
                                    SystemSettings systemSettings,
                                    Database database,
                                    Caches caches,
                                    ProjectConfig projectConfig,
                                    DataSource dataSource,
                                    DatabaseLockManager lockManager )
    {
        LOGGER.debug( "getReader called on dataSource {}", dataSource );
        DataSource.DataDisposition disposition = dataSource.getDisposition();
        Source source;

        switch ( disposition )
        {
            case DATACARD:
                source = new DatacardSource( timeSeriesIngester,
                                             dataSource );
                break;
            case XML_FI_TIMESERIES:
            case XML_PI_TIMESERIES:
                source = new FEWSSource( timeSeriesIngester,
                                         dataSource );
                break;
            case JSON_WRDS_AHPS:
            case JSON_WRDS_NWM:
                source = new WRDSSource( timeSeriesIngester,
                                         projectConfig,
                                         dataSource,
                                         systemSettings );
                break;
            case CSV_WRES:
                source = new CSVSource( timeSeriesIngester,
                                        dataSource,
                                        systemSettings );
                break;
            case JSON_WATERML:
                source = new WaterMLBasicSource( timeSeriesIngester,
                                                 dataSource );
                break;
            case GZIP:
                source = new ZippedSource( timeSeriesIngester,
                                           systemSettings,
                                           database,
                                           caches,
                                           projectConfig,
                                           dataSource,
                                           lockManager );
                break;
            case NETCDF_GRIDDED:
                source = new GriddedNWMSource( timeSeriesIngester,
                                               systemSettings,
                                               projectConfig,
                                               dataSource );
                break;
            default:
                String message = "The uri '%s' is not a valid source of data.";
                throw new IllegalArgumentException( String.format( message, dataSource.getUri() ) );
        }

        return source;
    }
    
    private ReaderFactory()
    {
    }
}

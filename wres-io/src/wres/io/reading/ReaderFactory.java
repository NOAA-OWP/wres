package wres.io.reading;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.commaseparated.CSVSource;
import wres.io.reading.datacard.DatacardSource;
import wres.io.reading.fews.FEWSSource;
import wres.io.reading.nwm.NWMSource;
import wres.io.reading.waterml.WaterMLBasicSource;
import wres.io.reading.wrds.WRDSSource;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * @author ctubbs
 *
 */
public class ReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger( ReaderFactory.class );
    private ReaderFactory(){}

    public static BasicSource getReader( SystemSettings systemSettings,
                                         Database database,
                                         DataSources dataSourcesCache,
                                         Features featuresCache,
                                         Variables variablesCache,
                                         Ensembles ensemblesCache,
                                         MeasurementUnits measurementUnitsCache,
                                         ProjectConfig projectConfig,
                                         DataSource dataSource,
                                         DatabaseLockManager lockManager )
            throws IOException
	{
        LOGGER.debug( "getReader called on dataSource {}", dataSource );
        DataSource.DataDisposition disposition = dataSource.getDisposition();
		BasicSource source;

        switch ( disposition )
        {
			case DATACARD:
                source = new DatacardSource( systemSettings,
                                             database,
                                             featuresCache,
                                             variablesCache,
                                             ensemblesCache,
                                             measurementUnitsCache,
                                             projectConfig,
                                             dataSource,
                                             lockManager );
				break;
            case GZIP:
                source = new ZippedSource( systemSettings,
                                           database,
                                           dataSourcesCache,
                                           featuresCache,
                                           variablesCache,
                                           ensemblesCache,
                                           measurementUnitsCache,
                                           projectConfig,
                                           dataSource,
                                           lockManager );
				break;
            case NETCDF_GRIDDED:
                source = new NWMSource( systemSettings,
                                        database,
                                        dataSourcesCache,
                                        projectConfig,
                                        dataSource,
                                        lockManager );
				break;
            case XML_PI_TIMESERIES:
                source = new FEWSSource( systemSettings,
                                         database,
                                         dataSourcesCache,
                                         featuresCache,
                                         variablesCache,
                                         ensemblesCache,
                                         measurementUnitsCache,
                                         projectConfig,
                                         dataSource,
                                         lockManager );
				break;
            case JSON_WRDS_AHPS:
            case JSON_WRDS_NWM:
                source = new WRDSSource( systemSettings,
                                         database,
                                         dataSourcesCache,
                                         featuresCache,
                                         variablesCache,
                                         ensemblesCache,
                                         measurementUnitsCache,
                                         projectConfig,
                                         dataSource,
                                         lockManager );
                break;
            case CSV_WRES:
                source = new CSVSource( systemSettings,
                                        database,
                                        featuresCache,
                                        variablesCache,
                                        ensemblesCache,
                                        measurementUnitsCache,
                                        projectConfig,
                                        dataSource,
                                        lockManager );
                break;
            case JSON_WATERML:
                source = new WaterMLBasicSource( systemSettings,
                                                 database,
                                                 featuresCache,
                                                 variablesCache,
                                                 ensemblesCache,
                                                 measurementUnitsCache,
                                                 projectConfig,
                                                 dataSource,
                                                 lockManager );
                break;
			default:
				String message = "The uri '%s' is not a valid source of data.";
				throw new IOException(String.format(message, dataSource.getUri()));
		}

		return source;
	}
}

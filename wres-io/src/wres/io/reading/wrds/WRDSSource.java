package wres.io.reading.wrds;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.InterfaceShortHand;
import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.WrdsNwmReader;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * Ingests JSON data from the WRDS schema from either the service or JSON files on the file system
 * <p>
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

    private final SystemSettings systemSettings;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final DatabaseLockManager lockManager;

    public WRDSSource( SystemSettings systemSettings,
                       Database database,
                       DataSources dataSourcesCache,
                       Features featuresCache,
                       Variables variablesCache,
                       Ensembles ensemblesCache,
                       MeasurementUnits measurementUnitsCache,
                       ProjectConfig projectConfig,
                       DataSource dataSource,
                       DatabaseLockManager lockManager )
    {
        super( projectConfig, dataSource );
        this.systemSettings = systemSettings;
        this.database = database;
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.lockManager = lockManager;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private DataSources getDataSourcesCache()
    {
        return this.dataSourcesCache;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private Variables getVariablesCache()
    {
        return this.variablesCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    @Override
    public List<IngestResult> save() throws IOException
    {
        InterfaceShortHand interfaceShortHand = this.getDataSource()
                                                    .getSource()
                                                    .getInterface();
        if ( Objects.nonNull( interfaceShortHand )
             && interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM ) )
        {
            WrdsNwmReader reader = new WrdsNwmReader( this.getSystemSettings(),
                                                      this.getDatabase(),
                                                      this.getFeaturesCache(),
                                                      this.getVariablesCache(),
                                                      this.getEnsemblesCache(),
                                                      this.getMeasurementUnitsCache(),
                                                      this.getProjectConfig(),
                                                      this.getDataSource(),
                                                      this.getLockManager() );
            return reader.call();
        }
        else
        {
            ReadValueManager reader =
                    createReadValueManager( this.getProjectConfig(),
                                            this.getDataSource(),
                                            this.lockManager );
            return reader.save();
        }
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

    ReadValueManager createReadValueManager( ProjectConfig projectConfig,
                                             DataSource dataSource,
                                             DatabaseLockManager lockManager )
    {
        return new ReadValueManager( this.getSystemSettings(),
                                     this.getDatabase(),
                                     this.getDataSourcesCache(),
                                     this.getFeaturesCache(),
                                     this.getVariablesCache(),
                                     this.getEnsemblesCache(),
                                     this.getMeasurementUnitsCache(),
                                     projectConfig,
                                     dataSource,
                                     lockManager );
    }
}

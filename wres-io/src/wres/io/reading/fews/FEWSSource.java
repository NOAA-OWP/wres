package wres.io.reading.fews;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class FEWSSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FEWSSource.class );

    private final SystemSettings systemSettings;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final DatabaseLockManager lockManager;

	/**
     * Constructor that sets the filename
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param dataSourcesCache The data sources cache to use.
     * @param featuresCache The features cache to use.
     * @param variablesCache The variables cache to use.
     * @param ensemblesCache The ensembles cache to use.
     * @param measurementUnitsCache The measurement units cache to use.
     * @param projectConfig the ProjectConfig causing ingest
     * @param dataSource the data source information
     * @param lockManager the tool to manage ingest locks, shared per ingest
	 */
    public FEWSSource( SystemSettings systemSettings,
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

    @Override
    public List<IngestResult> save() throws IOException
    {
        try ( PIXMLReader sourceReader = new PIXMLReader( this.getSystemSettings(),
                                                          this.getDatabase(),
                                                          this.getDataSourcesCache(),
                                                          this.getFeaturesCache(),
                                                          this.getVariablesCache(),
                                                          this.getEnsemblesCache(),
                                                          this.getMeasurementUnitsCache(),
                                                          this.getProjectConfig(),
                                                          this.dataSource,
                                                          this.getHash(),
                                                          this.getLockManager() )
        )
        {
            sourceReader.parse();
            return sourceReader.getIngestResults();
        }
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    @Override
    protected Logger getLogger()
    {
        return FEWSSource.LOGGER;
    }

}

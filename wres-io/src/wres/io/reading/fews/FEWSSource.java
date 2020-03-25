package wres.io.reading.fews;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
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
        boolean anotherTaskInChargeOfIngest;
        boolean ingestFullyCompleted;
        int id;
        DataSources dataSources = this.getDataSourcesCache();

        try
        {
            // This is an awkward inference: "cache presence means I ingest."
            // See #50933-420
            if ( !dataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader = new PIXMLReader( this.getSystemSettings(),
                                                            this.getDatabase(),
                                                            this.getDataSourcesCache(),
                                                            this.getFeaturesCache(),
                                                            this.getVariablesCache(),
                                                            this.getEnsemblesCache(),
                                                            this.getMeasurementUnitsCache(),
                                                            this.getFilename(),
                                                            this.getHash(),
                                                            this.getLockManager() );
                sourceReader.setDataSourceConfig( this.getDataSourceConfig() );
                sourceReader.setSourceConfig( this.getSourceConfig() );
                sourceReader.parse();
                id = sourceReader.getLastSourceId();
                anotherTaskInChargeOfIngest = !sourceReader.inChargeOfIngest();
                ingestFullyCompleted = sourceReader.ingestFullyCompleted();
            }
            else
            {
                anotherTaskInChargeOfIngest = true;
                SourceDetails sourceDetails = dataSources.getExistingSource( this.getHash() );
                id = sourceDetails.getId();
                Database database = this.getDatabase();
                SourceCompletedDetails completedDetails =
                        new SourceCompletedDetails( database,
                                                    sourceDetails );
                ingestFullyCompleted = completedDetails.wasCompleted();
            }
        }
        catch ( SQLException se )
        {
            String message = "While saving the";

            if (ConfigHelper.isForecast( this.getDataSourceConfig() ))
            {
                message += " forecast ";
            }
            else
            {
                message += " observation ";
            }

            message += "from source '" +
                       this.getAbsoluteFilename() +
                       "', encountered an issue.";

            throw new IngestException( message, se );
        }

        LOGGER.debug("Finished Parsing '{}'", this.getFilename());

        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                this.getDataSource(),
                                                id,
                                                anotherTaskInChargeOfIngest,
                                                !ingestFullyCompleted );
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

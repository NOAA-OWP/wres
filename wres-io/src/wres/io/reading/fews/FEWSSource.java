package wres.io.reading.fews;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.system.DatabaseLockManager;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class FEWSSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FEWSSource.class );
    private final DatabaseLockManager lockManager;

	/**
     * Constructor that sets the filename
     * @param projectConfig the ProjectConfig causing ingest
     * @param dataSource the data source information
     * @param lockManager the tool to manage ingest locks, shared per ingest
	 */
    public FEWSSource( ProjectConfig projectConfig,
                       DataSource dataSource,
                       DatabaseLockManager lockManager )
    {
        super( projectConfig, dataSource );
		this.lockManager = lockManager;
	}

    @Override
    public List<IngestResult> save() throws IOException
    {
        boolean anotherTaskInChargeOfIngest;
        boolean ingestFullyCompleted;
        int id;

        try
        {
            // This is an awkward inference: "cache presence means I ingest."
            // See #50933-420
            if ( !DataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader = new PIXMLReader( this.getFilename(),
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
                SourceDetails sourceDetails = DataSources.getExistingSource( this.getHash() );
                id = sourceDetails.getId();
                SourceCompletedDetails completedDetails =
                        new SourceCompletedDetails( sourceDetails );
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

package wres.io.reading.fews;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class FEWSSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FEWSSource.class );

	/**
     * Constructor that sets the filename
     * @param projectConfig the ProjectConfig causing ingest
	 * @param filename The name of the source file
	 */
    public FEWSSource( ProjectConfig projectConfig,
                       String filename )
    {
        super( projectConfig );
		this.setFilename(filename);
		this.setHash();
	}

    @Override
    public List<IngestResult> save() throws IOException
    {
        boolean wasFoundInCache;
        try
        {
            if ( !DataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader = new PIXMLReader( this.getAbsoluteFilename(),
                                                            this.getHash() );
                sourceReader.setDataSourceConfig( this.getDataSourceConfig() );
                sourceReader.setSpecifiedFeatures( this.getSpecifiedFeatures() );
                sourceReader.setSourceConfig( this.getSourceConfig() );
                sourceReader.parse();
                wasFoundInCache = false;
            }
            else
            {
                wasFoundInCache = true;
            }
        }
        catch ( SQLException se )
        {
            String message = "While saving the";

            if (ConfigHelper.isForecast( this.dataSourceConfig ))
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
                                                this.getDataSourceConfig(),
                                                this.getHash(),
                                                wasFoundInCache );
    }

    @Override
    protected Logger getLogger()
    {
        return FEWSSource.LOGGER;
    }

}

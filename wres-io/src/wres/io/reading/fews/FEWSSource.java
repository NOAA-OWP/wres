package wres.io.reading.fews;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.util.Internal;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
@Internal(exclusivePackage = "wres.io")
public class FEWSSource extends BasicSource
{
	/**
     * Constructor that sets the filename
     * @param projectConfig the ProjectConfig causing ingest
	 * @param filename The name of the source file
	 */
    @Internal(exclusivePackage = "wres.io")
    public FEWSSource( ProjectConfig projectConfig,
                       String filename )
    {
        super( projectConfig );
		this.setFilename(filename);
		this.setHash();
	}

	@Override
    public List<IngestResult> saveForecast() throws IOException
    {
        boolean wasFoundInCache;
        try
        {
            // TODO: This will break if we have the source but not the variable/location
            if ( !DataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader = new PIXMLReader( this.getFilename(),
                                                            true,
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
            String message = "While saving the forecast from source "
                             + this.getAbsoluteFilename()
                             + ", encountered an issue.";
            throw new IngestException( message, se );
        }

        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                this.getDataSourceConfig(),
                                                this.getHash(),
                                                wasFoundInCache );
    }

    @Override
    public List<IngestResult> saveObservation() throws IOException
    {
        boolean wasFoundInCache;
        try
        {
            if ( !DataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader =
                        new PIXMLReader( this.getAbsoluteFilename(),
                                         false,
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
            String message = "While saving the observation from source "
                             + this.getAbsoluteFilename()
                             + ", encountered an issue.";
            throw new IngestException( message, se );
        }

        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                this.getDataSourceConfig(),
                                                this.getHash(),
                                                wasFoundInCache );
    }

}

package wres.io.reading.fews;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.caching.DataSources;
import wres.io.reading.BasicSource;
import wres.util.Internal;
import wres.util.Strings;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
@Internal(exclusivePackage = "wres.io")
public class FEWSSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FEWSSource.class );
	/**
	 * Constructor that sets the filename 
	 * @param filename The name of the source file
	 */
    @Internal(exclusivePackage = "wres.io")
	public FEWSSource(String filename)
	{
		this.setFilename(filename);
		this.setHash();
	}

	@Override
	public void saveForecast() throws IOException
    {
        try
        {
            if ( !DataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader = new PIXMLReader( this.getFilename(),
                                                            true,
                                                            this.getHash(),
                                                            this.getProjectDetails() );
                sourceReader.setDataSourceConfig( this.getDataSourceConfig() );
                sourceReader.setSpecifiedFeatures( this.getSpecifiedFeatures() );
                sourceReader.parse();
            }
            else
            {
                this.getProjectDetails().addSource( this.getHash(), getDataSourceConfig() );
            }
        }
        catch ( SQLException | ExecutionException | InterruptedException e )
        {
            LOGGER.error( Strings.getStackTrace(e) );
        }
    }

    @Override
	public void saveObservation() throws IOException {
        try
        {
            if ( !DataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader =
                        new PIXMLReader( this.getAbsoluteFilename(),
                                         false,
                                         this.getHash(),
                                         this.getProjectDetails() );
                sourceReader.setDataSourceConfig( this.getDataSourceConfig() );
                sourceReader.setSpecifiedFeatures( this.getSpecifiedFeatures() );
                sourceReader.parse();
            }
            else
            {
                this.getProjectDetails().addSource( this.getHash(), getDataSourceConfig() );
            }
        }
        catch ( InterruptedException | ExecutionException | SQLException e )
        {
            LOGGER.error( Strings.getStackTrace( e ) );
        }
    }

}

package wres.io.reading.fews;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import wres.io.data.caching.DataSources;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
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
	 * @param filename The name of the source file
	 */
    @Internal(exclusivePackage = "wres.io")
	public FEWSSource(String filename)
	{
		this.setFilename(filename);
		this.setHash();
	}

	@Override
	public List<String> saveForecast() throws IOException
    {
        try
        {
            // TODO: This will break if we have the source but not the variable/location
            if ( !DataSources.hasSource( this.getHash() ) )
            {
                PIXMLReader sourceReader = new PIXMLReader( this.getFilename(),
                                                            true,
                                                            this.getHash(),
                                                            this.getProjectDetails() );
                sourceReader.setDataSourceConfig( this.getDataSourceConfig() );
                sourceReader.setSpecifiedFeatures( this.getSpecifiedFeatures() );
                sourceReader.setSourceConfig( this.getSourceConfig() );
                sourceReader.parse();
            }
            else
            {
                this.getProjectDetails().addSource( this.getHash(), getDataSourceConfig() );
            }
        }
        catch ( SQLException se )
        {
            String message = "While saving the forecast from source "
                             + this.getAbsoluteFilename()
                             + ", encountered an issue.";
            throw new IngestException( message, se );
        }

        List<String> result = new ArrayList<>( 1 );
        result.add( this.getHash() );
        return Collections.unmodifiableList( result );
    }

    @Override
	public List<String> saveObservation() throws IOException
    {
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
                sourceReader.setSourceConfig( this.getSourceConfig() );
                sourceReader.parse();
            }
            else
            {
                this.getProjectDetails().addSource( this.getHash(), getDataSourceConfig() );
            }
        }
        catch ( SQLException se )
        {
            String message = "While saving the observation from source "
                             + this.getAbsoluteFilename()
                             + ", encountered an issue.";
            throw new IngestException( message, se );
        }

        List<String> result = new ArrayList<>( 1 );
        result.add( this.getHash() );
        return Collections.unmodifiableList( result );
    }

}

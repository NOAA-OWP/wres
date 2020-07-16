package wres.io.reading;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;

/**
 * @author Christopher Tubbs
 * Serves as the base class for all classes that are expected to save
 * observations or forecasts from a source file
 */
public abstract class BasicSource
{
    protected final ProjectConfig projectConfig;
    protected final DataSource dataSource;

    protected BasicSource( ProjectConfig projectConfig,
                           DataSource dataSource )
    {
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
    }

    public List<IngestResult> save() throws IOException
    {
        if ( ConfigHelper.isForecast( this.dataSource.getContext() ))
        {
            return this.saveForecast();
        }
        else
        {
            return this.saveObservation();
        }
    }

    /**
     * Saves data within the source file as a forecast
     * 
     * @return the ingest results
     * @throws IOException always, because this is not implemented
     */
    protected List<IngestResult> saveForecast() throws IOException
	{
		throw new IOException("Forecasts may not be saved using this type of source.");
	}

    /**
     * Saves data within the source file as an observation
     * 
     * @return the ingest results
     * @throws IOException always, because this is not implemented
     */
    protected List<IngestResult> saveObservation() throws IOException
	{
		throw new IOException("Observations may not be saved using this type of source.");
	}

    /**
     * @return The name of the file that contains the given source data
     */
	public URI getFilename()
	{
        return this.dataSource.getUri() ;
	}

    /**
     * @return The absolute path of the file to read
     */
	protected String getAbsoluteFilename()
	{
		if (absoluteFilename == null)
		{
            absoluteFilename = Paths.get(getFilename()).toAbsolutePath().toString();
		}
		return absoluteFilename;
	}

    /**
     * @return The configured specification indicating what to ingest and how
     */
    protected DataSourceConfig getDataSourceConfig ()
    {
        return this.dataSource.getContext();
    }

    protected DataSourceConfig.Source getSourceConfig()
    {
        return this.dataSource.getSource();
    }

    protected ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    protected DataSource getDataSource()
    {
        return this.dataSource;
    }

    /**
     * The MD5 hash of the given file
     */
	private String hash;

    /**
     * The absolute path to the file containing the given source data
     */
	private String absoluteFilename;


    /**
     * @return The name of the variable to ingest
     */
	protected String getSpecifiedVariableName()
    {
        DataSourceConfig.Variable variable = this.getDataSource()
                                                 .getContext()
                                                 .getVariable();
        if ( variable != null )
        {
            return variable.getValue();
        }

        return null;
    }


    /**
     * @return The value specifying a value that is missing from the data set
     * originating from the data source configuration. While parsing the data,
     * if this value is encountered, it indicates that the value should be
     * ignored as it represents invalid data. This should be ignored in data
     * sources that define their own missing value.
     */
    protected String getSpecifiedMissingValue()
    {
        String missingValue = null;

        DataSourceConfig.Source source = this.getDataSource()
                                             .getSource();

        if ( source.getMissingValue() != null && !source.getMissingValue().isEmpty() )
        {
            missingValue = source.getMissingValue();

            if ( missingValue.lastIndexOf( '.' ) + 6 < missingValue.length() )
            {
                missingValue = missingValue.substring( 0, missingValue.lastIndexOf( '.' ) + 6 );
            }
        }

        return missingValue;
    }

    /**
     * Retrieves the results of the asynchrous hashing operation for the file
     * @return The MD5 hash of the contents of the current source file
     * @throws UnsupportedOperationException when hash was not previously requested?
     */
    protected String getHash()
    {
        return this.hash;
    }

    public void setHash(String hash)
    {
        this.hash = hash;
    }

	protected abstract Logger getLogger();
}

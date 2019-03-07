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

    /**
        System agnostic newline character used to make created scripts easier
        to read
     */
    protected static final String NEWLINE = System.lineSeparator();

    /**
     * Epsilon value used to test floating point equivalency
     */
    protected static final double EPSILON = 0.0000001;

    protected final ProjectConfig projectConfig;

    protected BasicSource( ProjectConfig projectConfig )
    {
        this.projectConfig = projectConfig;
    }

    public List<IngestResult> save() throws IOException
    {
        if ( ConfigHelper.isForecast( this.dataSourceConfig ))
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
		return filename;
	}

    /**
     * Sets the name of the file containing the data to read
     * @param name The name of the file to read
     */
	protected void setFilename ( URI name )
	{
		filename = name;
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
     * Loads the specification for the data source
     * @param dataSourceConfig the outer data source config, such as "right"
     */
	public void setDataSourceConfig (DataSourceConfig dataSourceConfig)
	{
		this.dataSourceConfig = dataSourceConfig;
	}

    /**
     * @return The configured specification indicating what to ingest and how
     */
	protected DataSourceConfig getDataSourceConfig ()
	{
		return this.dataSourceConfig;
	}

    public void setSourceConfig ( DataSourceConfig.Source sourceConfig )
    {
        this.sourceConfig = sourceConfig;
    }

    protected DataSourceConfig.Source getSourceConfig()
    {
        return this.sourceConfig;
    }

    protected ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    public void setIsRemote(final boolean isRemote)
    {
        this.isRemote = isRemote;
    }

    /**
     * The name of the file containing the given source data
     */
	protected URI filename;

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
        String variableName = null;

        if (dataSourceConfig != null) {
            variableName = dataSourceConfig.getVariable().getValue();
        }

        return variableName;
    }

    /**
     * @return The suggested measurement unit of the variable to ingest.
     * This should be ignored within data sources that define their own measurement unit.
     */
    protected String getSpecifiedVariableUnit()
    {
        String unit = null;

        if ( this.getSourceConfig() != null )
        {
            DataSourceConfig.Source source = this.getSourceConfig();

            if ( source.getUnit() != null && !source.getUnit().isEmpty() ) {
                unit = source.getUnit();
            }
        }

        return unit;
    }

    /**
     * @return The specific location ID given by the source tag within the
     * data source configuration. This should be ignored in data sources that
     * define their own locations.
     */
    protected String getSpecifiedLocationID()
    {
        String locationID = null;

        if ( this.getSourceConfig() != null )
        {
            DataSourceConfig.Source source = this.getSourceConfig();

            if ( source.getLocationId() != null && !source.getLocationId().isEmpty() )
            {
                locationID = source.getLocationId();
            }
        }

        return locationID;
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

        if ( this.getSourceConfig() != null )
        {
            DataSourceConfig.Source source = this.getSourceConfig();

            if ( source.getMissingValue() != null && !source.getMissingValue().isEmpty() )
            {
                missingValue = source.getMissingValue();

                if ( missingValue.lastIndexOf( '.' ) + 6 < missingValue.length() )
                {
                    missingValue = missingValue.substring( 0, missingValue.lastIndexOf( '.' ) + 6 );
                }
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

    /**
     * The configuration of the data source indicating that this file might
     * need to be ingested
     */
	protected DataSourceConfig dataSourceConfig;

    /**
     * The precise configuration of the data source indicating that this file
     * might need to be ingested
     */
    private DataSourceConfig.Source sourceConfig;

    /**
     * Whether or not the data is held remotely
     */
	private boolean isRemote;

	protected abstract Logger getLogger();
}

package wres.io.reading;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.concurrency.Executor;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.NotImplementedException;
import wres.util.Strings;

/**
 * @author Christopher Tubbs
 * Serves as the base class for all classes that are expected to save
 * observations or forecasts from a source file
 */
@Internal(exclusivePackage = "wres.io")
public abstract class BasicSource {

    /**
        System agnostic newline character used to make created scripts easier
        to read
     */
    protected static final String NEWLINE = System.lineSeparator();
	
	@SuppressWarnings("static-method")
    /**
     * Saves data within the source file as a forecast
     */
    public void saveForecast() throws IOException
	{
		throw new IOException("Forecasts may not be saved using this type of source.");
	}
	
	@SuppressWarnings("static-method")
    /**
     * Saves data within the source file as an observation
     */
    public void saveObservation() throws IOException
	{
		throw new IOException("Observations may not be saved using this type of source.");
	}

    /**
     * @return The name of the file that contains the given source data
     */
	public String getFilename()
	{
		return filename;
	}

    /**
     * Sets the name of the file containing the data to read
     * @param name The name of the file to read
     */
	protected void setFilename (String name)
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

    /**
     * Sets the specific features to ingest. Only the described features should
     * be ingested
     * @param specifiedFeatures A listing of features used to filter the ingest
     *                          process
     */
	public void setSpecifiedFeatures(List<Feature> specifiedFeatures)
    {
        this.specifiedFeatures = specifiedFeatures;
    }

    /**
     * @return The listing of configured features that may be ingested
     */
    protected List<Feature> getSpecifiedFeatures()
    {
        return this.specifiedFeatures;
    }

    /**
     * The name of the file containing the given source data
     */
	protected String filename = "";

    /**
     * The MD5 hash of the given file
     */
	private String hash;

    /**
     * The task that will compute the hash of the file
     */
	private Future<String> futureHash;

    /**
     * The absolute path to the file containing the given source data
     */
	private String absoluteFilename;

    /**
     * Details linking a configured project to details within the database
     */
	private ProjectDetails projectDetails;

    /**
     * Sets the details linking a configured project to data within the database
     * @param projectDetails The details linking the configured project to
     *                       the database
     */
	public void setProjectDetails(ProjectDetails projectDetails)
    {
        this.projectDetails = projectDetails;
    }

    /**
     * @return Details describing data that pertains to the configured project
     */
    protected ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

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

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, this.filename);

            if (source != null && source.getUnit() != null && !source.getUnit().isEmpty()) {
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

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, this.filename);

            if (source != null && source.getLocation() != null && !source.getLocation().isEmpty())
            {
                locationID = source.getLocation();
            }
        }

        return locationID;
    }

    /**
     * @return The intended time zone specified within the configuration for the
     * data source. This should be ignored in data sources that define their
     * own time zone.
     */
    protected ZoneId getSpecifiedTimeZone()
    {
        ZoneId timeZone = null;

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, this.filename);

            if ( source != null
                 && source.getTimeZone() != null
                 && !source.getTimeZone().isEmpty() )
            {
                timeZone = ZoneId.of( source.getTimeZone() );
            }
        }

        return timeZone;
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

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, this.filename);

            if (source != null && source.getMissingValue() != null && !source.getMissingValue().isEmpty())
            {
                missingValue = source.getMissingValue();
            }
        }

        return missingValue;
    }

    /**
     * Determines whether or not the data at the given location or with the
     * given contents from the given configuration should be ingested into
     * the database.
     * @param filePath The path to the file on the file system
     * @param source The configuration indicating the location of the file
     * @param contents optional read contents from the source file. Used when
     *                 the source originates from an archive.
     * @return Whether or not to ingest the file
     */
    boolean shouldIngest( String filePath, DataSourceConfig.Source source, byte[] contents )
    {
        SourceType specifiedFormat = ReaderFactory.getFileType(source.getFormat());
        SourceType pathFormat = ReaderFactory.getFiletype(filePath);

        boolean ingest = specifiedFormat == SourceType.UNDEFINED || specifiedFormat.equals(pathFormat);

        if (ingest)
        {
            try
            {
                ingest = !dataExists(filePath, contents);
            }
            catch (SQLException e)
            {
                ingest = false;
            }
        }

        return ingest;
    }

    /**
     * Retrieves the results of the asynchrous hashing operation for the file
     * @return The MD5 hash of the contents of the current source file
     * @throws IOException when anything goes wrong while getting the hash
     */
    protected String getHash() throws IOException
    {
        if (this.hash == null)
        {
            if (this.futureHash != null)
            {
                try
                {
                    this.hash = this.futureHash.get();
                }
                catch ( InterruptedException ie )
                {
                    Thread.currentThread().interrupt();
                }
                catch ( ExecutionException ee )
                {
                    String message = "While identifying data from source "
                                     + this.getAbsoluteFilename()
                                     + ", encountered an issue.";
                    throw new IOException( message, ee );
                }
            }
            else
            {
                throw new NotImplementedException(
                        "No hashing operation was created during file ingestion. No hash could be retrieved."
                );
            }
        }
        return this.hash;
    }

    /**
     * @return The task that is hashing the file asynchronously
     */
    protected Future<String> getFutureHash()
    {
        return this.futureHash;
    }

    /**
     * Creates and executes the asynchronous task that will create the hash
     * of the passed contents. Generally used to hash data originating from
     * and archive.
     * @param contents The contents of the file to hash
     */
    protected void setHash(byte[] contents)
    {
        WRESCallable<String> hasher = new WRESCallable<String>() {
            @Override
            protected String execute() throws Exception
            {
                return Strings.getMD5Checksum( this.contentsToHash );
            }

            @Override
            protected Logger getLogger()
            {
                return null;
            }

            private byte[] contentsToHash;
            public WRESCallable<String> init(byte[] contentsToHash)
            {
                this.contentsToHash = contentsToHash;
                return this;
            }
        }.init( contents );
        this.futureHash = Executor.submitHighPriorityTask( hasher );
    }

    /**
     * Creates and executes an asynchronous task that will determine the hash
     * of a file given the path of the current file
     */
    protected void setHash()
    {
        WRESCallable<String> hasher = new WRESCallable<String>() {
            @Override
            protected String execute() throws Exception
            {
                return Strings.getMD5Checksum( this.fileNameToHash );
            }

            @Override
            protected Logger getLogger()
            {
                return null;
            }

            private String fileNameToHash;
            public WRESCallable<String> init(String fileNameToHash)
            {
                this.fileNameToHash = fileNameToHash;
                return this;
            }
        }.init( this.getFilename() );
        this.futureHash = Executor.submitHighPriorityTask( hasher );
    }

    // TODO: This process is now invalid; we need to rely on loaded
    // project information
    /**
     * Determines if the hash of the passed in contents are contained within the
     * database
     * @param sourceName The name of the file to hash
     * @param contents The contents of the file to hash
     * @return Whether or not the indicated data lies within the database
     * @throws SQLException Thrown if an error occurs while communicating with
     * the database
     */
    private boolean dataExists(String sourceName, byte[] contents)
            throws SQLException
    {
        StringBuilder script = new StringBuilder();

        script.append("SELECT EXISTS (").append(NEWLINE);
        script.append("     SELECT 1").append(NEWLINE);

        if (ConfigHelper.isForecast(dataSourceConfig))
        {
            script.append("     FROM wres.TimeSeries TS").append(NEWLINE);
            script.append("     INNER JOIN wres.ForecastSource SL").append(NEWLINE);
            script.append("         ON SL.forecast_id = TS.timeseries_id").append(NEWLINE);
            script.append("     INNER JOIN wres.VariablePosition VP").append(NEWLINE);
            script.append("         ON VP.variableposition_id = TS.variableposition_id").append(NEWLINE);
        }
        else
        {
            script.append("     FROM wres.Observation SL").append(NEWLINE);
            script.append("     INNER JOIN wres.VariablePosition VP").append(NEWLINE);
            script.append("         ON VP.variableposition_id = SL.variableposition_id").append(NEWLINE);
        }

        script.append("     INNER JOIN wres.Source S").append(NEWLINE);
        script.append("         ON S.source_id = SL.source_id").append(NEWLINE);
        script.append("     INNER JOIN wres.Variable V").append(NEWLINE);
        script.append("         ON VP.variable_id = V.variable_id").append(NEWLINE);

        if (contents != null)
        {
            script.append("     WHERE S.hash = '")
                  .append( Strings.getMD5Checksum( contents ) )
                  .append("'")
                  .append(NEWLINE);
        }
        else
        {
            script.append("     WHERE S.path = '").append(sourceName).append("'").append(NEWLINE);
        }

        script.append("         AND V.variable_name = '")
              .append(this.dataSourceConfig.getVariable().getValue())
              .append("'")
              .append(NEWLINE);
        script.append(");");

        return Database.getResult( script.toString(), "exists");
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
    protected DataSourceConfig.Source sourceConfig;

    /**
     * The listing of features to ingest
     */
	private List<Feature> specifiedFeatures;
}

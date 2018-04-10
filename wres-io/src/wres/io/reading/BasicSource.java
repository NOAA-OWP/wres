package wres.io.reading;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.util.Strings;

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
    private List<IngestResult> saveForecast() throws IOException
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

    protected ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
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

            if (source != null && source.getLocationId() != null && !source.getLocationId().isEmpty())
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

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, this.filename);

            if (source != null && source.getMissingValue() != null && !source.getMissingValue().isEmpty())
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
     * Conditions the passed in value and transforms it into a form suitable to
     * save into the database.
     * <p>
     *     If the passed in value is found to be within {@value EPSILON} of the
     *     specified missing value, the value 'null' is returned.
     * </p>
     * @param value The original value
     * @return The conditioned value that is safe to save to the database.
     */
    protected String getValueToSave(Double value)
    {
        if (value != null && getSpecifiedMissingValue() != null)
        {
            Double missing = Double.parseDouble( this.getSpecifiedMissingValue() );
            if ( Precision.equals( value, missing, EPSILON ))
            {
                value = null;
            }
        }

        if (value == null)
        {
            return "\\N";
        }
        else
        {
            return String.valueOf(value);
        }
    }

    /**
     * Determines whether or not the data at the given location or with the
     * given contents from the given configuration should be ingested into
     * the database.
     * @param filePath The path to the file on the file system
     * @param source The configuration indicating the location of the file
     * @param contents optional read contents from the source file. Used when
     *                 the source originates from an archive.
     * @return Whether or not to ingest the file and the resulting hash
     * @throws IngestException when an exception prevents determining status
     */
    Pair<Boolean,String> shouldIngest( String filePath,
                                                 DataSourceConfig.Source source,
                                                 byte[] contents )
            throws IngestException
    {
        Format specifiedFormat = source.getFormat();
        Format pathFormat = ReaderFactory.getFiletype( filePath );

        boolean ingest = specifiedFormat == null
                         || specifiedFormat.equals( pathFormat );

        String contentHash = null;

        if (ingest)
        {
            try
            {
                if ( contents != null )
                {
                    contentHash = Strings.getMD5Checksum( contents );
                }
                else
                {
                    contentHash = this.getHash();
                }

                ingest = !dataExists( contentHash );
            }
            catch ( IOException | SQLException e )
            {
                String message = "Failed to determine whether to ingest file "
                                 + filePath;
                throw new IngestException( message, e );
            }
        }

        return Pair.of( ingest, contentHash );
    }

    /**
     * Retrieves the results of the asynchrous hashing operation for the file
     * @return The MD5 hash of the contents of the current source file
     * @throws IOException when anything goes wrong while getting the hash
     * @throws UnsupportedOperationException when hash was not previously requested?
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
                throw new UnsupportedOperationException(
                        "No hashing operation was created during file ingestion"
                                + " for file '" + this.getAbsoluteFilename()
                                + "'. No hash could be retrieved."
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
        if (contents == null)
        {
            this.getLogger().debug( "A file ('{}') with no contents is being "
                                    + "attempted to be hashed.",
                                    this.getFilename() );
        }

        WRESCallable<String> hasher = new WRESCallable<String>()
        {
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
            WRESCallable<String> init(byte[] contentsToHash)
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
            WRESCallable<String> init(String fileNameToHash)
            {
                this.fileNameToHash = fileNameToHash;
                return this;
            }
        }.init( this.getFilename() );
        this.futureHash = Executor.submitHighPriorityTask( hasher );
    }

    /**
     * Determines if the source was already ingested into the database
     * @param contentHash The hash of the contents to look for
     * @return Whether or not the indicated data lies within the database
     * @throws SQLException Thrown if an error occurs while communicating with
     * the database
     * @throws NullPointerException when any arg is null
     */
    private boolean dataExists( String contentHash )
            throws SQLException
    {
        Objects.requireNonNull( contentHash );

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

        script.append("     WHERE S.hash = '")
              .append( contentHash )
              .append( "'" )
              .append( NEWLINE );

        script.append("         AND V.variable_name = '")
              .append(this.dataSourceConfig.getVariable().getValue())
              .append("'")
              .append(NEWLINE);
        script.append(");");

        return Database.getResult( script.toString(), "exists");
    }

    protected int getVariableId() throws SQLException
    {
        // We can compare to 0 because the ids in the database are > 0
        if (this.variableId == 0)
        {
            this.variableId = Variables.getVariableID(this.getDataSourceConfig());
        }

        return this.variableId;
    }

    protected int getMeasurementunitId() throws SQLException
    {
        if (this.measurementunitId == 0)
        {
            this.measurementunitId = Variables.getMeasurementUnitId( this.getVariableId() );
        }
        return this.measurementunitId;
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
     * The listing of features to ingest
     */
	private List<Feature> specifiedFeatures;

    /**
     * The ID of the variable being ingested
     */
	private int variableId;

    /**
     * The ID of the unit that the variable is measured in
     */
	private int measurementunitId;

	protected abstract Logger getLogger();
}

package wres.io.reading;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
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
 * @author ctubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public abstract class BasicSource {

    protected static final String NEWLINE = System.lineSeparator();
	
	@SuppressWarnings("static-method")
    public void saveForecast() throws IOException
	{
		throw new IOException("Forecasts may not be saved using this type of source.");
	}
	
	@SuppressWarnings("static-method")
    public void saveObservation() throws IOException
	{
		throw new IOException("Observations may not be saved using this type of source.");
	}

	public String getFilename()
	{
		return filename;
	}
	
	protected void setFilename (String name)
	{
		filename = name;
	}
	
	protected String getAbsoluteFilename()
	{
		if (absoluteFilename == null)
		{
            absoluteFilename = Paths.get(getFilename()).toAbsolutePath().toString();
		}
		return absoluteFilename;
	}

	public void setDataSourceConfig (DataSourceConfig dataSourceConfig)
	{
		this.dataSourceConfig = dataSourceConfig;
	}

	protected DataSourceConfig getDataSourceConfig ()
	{
		return this.dataSourceConfig;
	}

	public void setSpecifiedFeatures(List<Feature> specifiedFeatures)
    {
        this.specifiedFeatures = specifiedFeatures;
    }

    protected List<Feature> getSpecifiedFeatures()
    {
        return this.specifiedFeatures;
    }

	protected String filename = "";
	private String hash;
	private Future<String> futureHash;
	private String absoluteFilename;
	private ProjectDetails projectDetails;

	public void setProjectDetails(ProjectDetails projectDetails)
    {
        this.projectDetails = projectDetails;
    }

    protected ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

	protected String getSpecifiedVariableName()
    {
        String variableName = null;

        if (dataSourceConfig != null) {
            variableName = dataSourceConfig.getVariable().getValue();
        }

        return variableName;
    }

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

    protected String getSpecifiedTimeZone()
    {
        String timeZone = "UTC";

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, this.filename);

            if (source != null && source.getTimeZone() != null && !source.getTimeZone().value().isEmpty())
            {
                timeZone = source.getTimeZone().value();
            }
        }

        return timeZone;
    }

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

    boolean shouldIngest( String filePath, DataSourceConfig.Source source, byte[] contents )
    {
        SourceType specifiedFormat = ReaderFactory.getFileType(source.getFormat());
        SourceType pathFormat = ReaderFactory.getFiletype(filePath);

        boolean ingest = specifiedFormat == SourceType.UNDEFINED || specifiedFormat.equals(pathFormat);

        if (ingest)
        {
            try {
                ingest = !dataExists(filePath, contents);
            }
            catch (SQLException | InterruptedException | ExecutionException e) {
                ingest = false;
            }
        }

        return ingest;
    }

    protected String getHash() throws ExecutionException, InterruptedException
    {
        if (this.hash == null)
        {
            if (this.futureHash != null)
            {
                this.hash = this.futureHash.get();
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

    protected Future<String> getFutureHash()
    {
        return this.futureHash;
    }

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

    private boolean dataExists(String sourceName, byte[] contents)
            throws SQLException, ExecutionException, InterruptedException
    {
        StringBuilder script = new StringBuilder();

        script.append("SELECT EXISTS (").append(NEWLINE);
        script.append("     SELECT 1").append(NEWLINE);

        if (ConfigHelper.isForecast(dataSourceConfig))
        {
            script.append("     FROM wres.Forecast F").append(NEWLINE);
            script.append("     INNER JOIN wres.ForecastSource SL").append(NEWLINE);
            script.append("         ON SL.forecast_id = F.forecast_id").append(NEWLINE);
            script.append("     INNER JOIN wres.ForecastEnsemble FE").append(NEWLINE);
            script.append("         ON FE.forecast_id = F.forecast_id").append(NEWLINE);
            script.append("     INNER JOIN wres.VariablePosition VP").append(NEWLINE);
            script.append("         ON VP.variableposition_id = FE.variableposition_id").append(NEWLINE);
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

	protected DataSourceConfig dataSourceConfig;
	private List<Feature> specifiedFeatures;
}

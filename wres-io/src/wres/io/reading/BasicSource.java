package wres.io.reading;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.io.utilities.Database;
import wres.util.Internal;

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

	public void setSpecifiedFeatures(List<Conditions.Feature> specifiedFeatures)
    {
        this.specifiedFeatures = specifiedFeatures;
    }

    protected List<Conditions.Feature> getSpecifiedFeatures()
    {
        return this.specifiedFeatures;
    }

	private String filename = "";
	private String absoluteFilename;

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

    private String getSpecifiedMissingValue()
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

    protected boolean shouldIngest(String filePath, DataSourceConfig.Source source)
    {
        SourceType specifiedFormat = ReaderFactory.getFileType(source.getFormat());
        SourceType pathFormat = ReaderFactory.getFiletype(filePath);

        boolean ingest = specifiedFormat == SourceType.UNDEFINED || specifiedFormat.equals(pathFormat);

        if (ingest)
        {
            try {
                ingest = !dataExists(filePath);
            }
            catch (SQLException e) {
                //this.getLogger().error( Strings.getStackTrace( e));
                ingest = false;
            }
        }

        return ingest;
    }

    private boolean dataExists(String sourceName) throws SQLException {
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
        script.append("     WHERE S.path = '").append(sourceName).append("'").append(NEWLINE);
        script.append("         AND V.variable_name = '")
              .append(this.dataSourceConfig.getVariable().getValue())
              .append("'")
              .append(NEWLINE);
        script.append(");");

        return Database.getResult( script.toString(), "exists");
    }

	private DataSourceConfig dataSourceConfig;
	private List<Conditions.Feature> specifiedFeatures;
}

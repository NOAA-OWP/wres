package wres.io.reading;

import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * @author ctubbs
 *
 */
public abstract class BasicSource {
	
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
	
	public SourceType getSourceType()
	{
		return sourceType;
	}
	
	protected void setSourceType(SourceType type)
	{
        sourceType = type;
	}

	public void setDataSourceConfig (DataSourceConfig dataSourceConfig)
	{
		this.dataSourceConfig = dataSourceConfig;
	}

	protected DataSourceConfig getDataSourceConfig ()
	{
		return this.dataSourceConfig;
	}
	
	private String filename = "";
	private String absoluteFilename;
	private SourceType sourceType = SourceType.UNDEFINED;

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

    private String getSpecifiedLocationID()
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

    private String getSpecifiedTimeZone()
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

	private DataSourceConfig dataSourceConfig;
}

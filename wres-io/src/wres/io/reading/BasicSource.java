package wres.io.reading;

import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.util.Internal;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author ctubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
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
	private List<Conditions.Feature> specifiedFeatures;
}

package wres.io.data.details;

import wres.io.data.caching.DataSources;
import wres.io.data.caching.ForecastTypes;
import wres.io.utilities.Database;
import wres.util.Internal;

import java.sql.SQLException;

/**
 * Important details about a forecast that predicted values for different variables over some span of time
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class ForecastDetails {
	private final static String NEWLINE = System.lineSeparator();
	
	private String sourcePath = null;
	private String forecastDate = null;

	private Integer forecast_id = null;
	private String creationDate = null;
	private String range = null;
	private Integer lead = null;

	/**
	 * The path to the file that contains data for the forecast
	 * @param path The path to the forecast file on the file system
	 */
	@Internal(exclusivePackage = "wres.io")
	public ForecastDetails(String path) {
		this.sourcePath = path;
	}

	/**
	 * Sets the date of when the forecast was generated
	 * @param forecastDate The value to update the current forecast date with
	 */
	public void setForecastDate(String forecastDate)
	{
		if (this.forecastDate == null || !this.forecastDate.equalsIgnoreCase(forecastDate))
		{
			this.forecastDate = forecastDate;
			forecast_id = null;
		}
	}

	public void setLead(int lead)
	{
		if (this.lead == null || this.lead != lead)
		{
			this.lead = lead;
			this.forecast_id = null;
		}
	}

	public void setCreationDate(String creationDate)
	{
	    if (this.creationDate == null || !this.creationDate.equalsIgnoreCase(creationDate)) {
	        this.creationDate = creationDate;
	        this.forecast_id = null;
	    }
	}
	
	public void setRange(String range)
	{
	    if (this.range == null || !this.range.equalsIgnoreCase(range))
	    {
	        this.range = range;
	        this.forecast_id = null;
	    }
	}
	
	/**
	 * Sets the path to the forecast file
	 * @param path The path to the forecast file on the file system
	 */
	public void setSourcePath(String path) {
		if (this.sourcePath == null || !this.sourcePath.equalsIgnoreCase(path)) {
			this.sourcePath = path;
			forecast_id = null;
		}
	}
	
	/**
	 * @return The ID of the forecast stored in the database
	 * @throws SQLException Thrown if the ID could not be loaded from the database
	 */
	public int getForecastID() throws SQLException {
		if (this.forecast_id == null)
		{
			save();
		}
		
		return this.forecast_id;
	}
	
	/**
	 * Saves the ID of the Forecast to the Forecast Details. The Forecast is added to the
	 * database if it does not currently exist
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	private void save() throws SQLException {
		String script = "";

		script += "WITH new_forecast AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.Forecast(forecast_date, forecasttype_id)" + NEWLINE;
		script += "		SELECT '" + forecastDate + "'," + NEWLINE;

		if (this.range == null)
        {
            script += "     null" + NEWLINE;
        }
        else
        {
            script += "     '" + String.valueOf(ForecastTypes.getForecastTypeId(this.range)) + "'" + NEWLINE;
        }

		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Forecast" + NEWLINE;
		script += "			WHERE forecast_date = '" + forecastDate + "'" + NEWLINE;
		script += "				AND forecasttype_id ";

		if (this.range == null)
        {
            script += "is null" + NEWLINE;
        }
        else {
            script += "= '" + String.valueOf(ForecastTypes.getForecastTypeId(this.range)) + "'" + NEWLINE;
        }

		script += "		)" + NEWLINE;
		script += "		RETURNING forecast_id" + NEWLINE;
		script += ")" + NEWLINE;
		script += "SELECT forecast_id" + NEWLINE;
		script += "FROM new_forecast" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
		script += "SELECT forecast_id" + NEWLINE;
		script += "FROM wres.Forecast" + NEWLINE;
		script += "WHERE forecast_date = '" + forecastDate + "'" + NEWLINE;
		script += "     AND forecasttype_id ";

        if (this.range == null)
        {
            script += "is null";
        }
        else {
            script += "= '" + String.valueOf(ForecastTypes.getForecastTypeId(this.range)) + "'";
        }

        script += ";";


		forecast_id = Database.getResult(script, "forecast_id");
		
		saveForecastSource();
	}
	
	private String getSourceDate() {
	    String date;
	    
	    if (this.creationDate != null)
	    {
	        date = this.creationDate;
	    }
	    else
	    {
	        date = this.forecastDate;
	    }
	    
	    return date;
	}
	
	/**
	 * Links the forecast the information about the source of its data in the database
	 * @throws SQLException Thrown if the Forecast and its source could not be properly linked
	 */
	private void saveForecastSource() throws SQLException {

        int sourceID = DataSources.getSourceID(sourcePath, getSourceDate(), this.lead);

        String script = "";
        script += "INSERT INTO wres.ForecastSource (forecast_id, source_id)" + NEWLINE;
        script += "SELECT " + this.forecast_id + ", " + sourceID + NEWLINE;
        script += "WHERE NOT EXISTS (" + NEWLINE;
        script += "     SELECT 1" + NEWLINE;
        script += "     FROM wres.ForecastSource" + NEWLINE;
        script += "     WHERE forecast_id = " + this.forecast_id + NEWLINE;
        script += "         AND source_id = " + sourceID + NEWLINE;
        script += ");";

		Database.execute(script);
	}
}

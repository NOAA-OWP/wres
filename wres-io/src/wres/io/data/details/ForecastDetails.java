package wres.io.data.details;

import java.sql.SQLException;

import wres.io.utilities.Database;
import wres.util.Internal;

/**
 * Important details about a forecast that predicted values for different variables over some span of time
 * @author Christopher Tubbs
 */
@Deprecated
@Internal(exclusivePackage = "wres.io")
public final class ForecastDetails {

	private final static String NEWLINE = System.lineSeparator();

	private String forecastDate = null;
	private String hash = null;

	private Integer forecast_id = null;
	private String creationDate = null;
	private String type = "";
	private Integer lead = null;
	private Integer projectID = null;

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

	public void setType(String type)
	{
		if (this.type == null || !this.type.equalsIgnoreCase( type ))
		{
			this.type = type;
			this.forecast_id = null;
		}
	}

	public void setHash(String hash)
	{
		if (this.hash == null || !this.hash.equalsIgnoreCase( hash ))
		{
			this.hash = hash;
			this.forecast_id = null;
		}
	}

	public void setProjectID(Integer projectID)
	{
		this.projectID = projectID;
	}

	public String getHash()
    {
        return this.hash;
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
		script += "		INSERT INTO wres.Forecast(forecast_date, scenario_id)" + NEWLINE;
		script += "		SELECT '" + this.forecastDate + "', ";
		script += "			" + this.projectID + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Forecast" + NEWLINE;
		script += "			WHERE forecast_date = '" + this.forecastDate + "'" + NEWLINE;
		script += "				AND scenario_id = " + this.projectID;
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
		script += "WHERE forecast_date = '" + this.forecastDate + "'" + NEWLINE;
		script += "     AND scenario_id = " + this.projectID + ";";

		this.forecast_id = Database.getResult(script, "forecast_id");
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
}

/**
 * 
 */
package data.details;

import java.sql.SQLException;

import data.SourceCache;
import util.Database;

/**
 * @author ctubbs
 *
 */
public final class ForecastDetails {
	private final static String newline = System.lineSeparator();
	
	private String sourcePath = null;
	private String forecast_date = null;
	private Integer forecast_id = null;
	
	public ForecastDetails(String path) {
		this.sourcePath = path;
	}
	
	public void set_forecast_date(String forecast_date)
	{
		if (this.forecast_date == null || !this.forecast_date.equalsIgnoreCase(forecast_date))
		{
			this.forecast_date = forecast_date;
			forecast_id = null;
		}
	}
	
	public void setSourcePath(String path) {
		if (this.sourcePath == null || !this.sourcePath.equalsIgnoreCase(path)) {
			this.sourcePath = path;
			forecast_id = null;
		}
	}
	
	public int get_forecast_id() throws Exception
	{
		if (forecast_id == null)
		{
			save();
		}
		
		return forecast_id;
	}
	
	public void save() throws Exception
	{
		String script = "";

		script += "WITH new_forecast AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.Forecast(forecast_date)" + newline;
		script += "		SELECT '" + forecast_date + "'" + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.Forecast" + newline;
		script += "			WHERE forecast_date = '" + forecast_date + "'" + newline;
		script += "		)" + newline;
		script += "		RETURNING forecast_id" + newline;
		script += ")" + newline;
		script += "SELECT forecast_id" + newline;
		script += "FROM new_forecast" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT forecast_id" + newline;
		script += "FROM wres.Forecast" + newline;
		script += "WHERE forecast_date = '" + forecast_date + "';";
		
		forecast_id = Database.get_result(script, "forecast_id");	
		
		// Uncomment when it is time to resume testing on ingest + source linking
		
		/*int sourceID = SourceCache.getSourceID(sourcePath, forecast_date);
		
		// Link the source to the forecast if there isn't one already
		script = "";
		script += "INSERT INTO wres.ForecastSource (forecast_id, source_id)" + newline;
		script += "SELECT " + this.forecast_id + ", " + sourceID + newline;
		script += "WHERE NOT EXISTS (" + newline;
		script += "		SELECT 1" + newline;
		script += "		FROM wres.ForecastSource" + newline;
		script += "		WHERE forecast_id = " + this.forecast_id + newline;
		script += "			AND source_id = " + sourceID + newline;
		script += ");";
		
		Database.execute(script);*/
	}
}

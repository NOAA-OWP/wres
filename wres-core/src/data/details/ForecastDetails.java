/**
 * 
 */
package data.details;

import java.sql.SQLException;

import util.Database;

/**
 * @author ctubbs
 *
 */
public final class ForecastDetails {
	private final static String newline = System.lineSeparator();
	
	private String forecast_date = null;
	private Integer forecast_id = null;
	
	public void set_forecast_date(String forecast_date)
	{
		if (this.forecast_date == null || !this.forecast_date.equalsIgnoreCase(forecast_date))
		{
			this.forecast_date = forecast_date;
			forecast_id = null;
		}
	}
	
	public int get_forecast_id() throws SQLException
	{
		if (forecast_id == null)
		{
			save();
		}
		
		return forecast_id;
	}
	
	public void save() throws SQLException
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
	}
}

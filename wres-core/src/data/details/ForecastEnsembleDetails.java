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
public final class ForecastEnsembleDetails {
	private static final String newline = System.lineSeparator();
	
	private Integer forecast_id = null;
	private Integer ensemble_id = null;
	private Integer variableposition_id = null;
	private Integer measurementunit_id = null;
	private Integer forecastensemble_id = null;
	
	public void set_forecast_id(int forecast_id)
	{
		if (this.forecast_id == null || this.forecast_id != forecast_id)
		{
			this.forecast_id = forecast_id;
			this.forecastensemble_id = null;
		}
	}
	
	public void set_ensemble_id(int ensemble_id)
	{
		if (this.ensemble_id == null || this.ensemble_id != ensemble_id)
		{
			this.ensemble_id = ensemble_id;
			this.forecastensemble_id = null;
		}
	}
	
	public void set_variableposition_id(int variableposition_id)
	{
		if (this.variableposition_id == null || this.variableposition_id != variableposition_id)
		{
			this.variableposition_id = variableposition_id;
			this.forecastensemble_id = null;
		}
	}
	
	public void set_measurementunit_id(int measurementunit_id)
	{
		if (this.measurementunit_id == null || this.measurementunit_id != measurementunit_id)
		{
			this.measurementunit_id = measurementunit_id;
			this.forecastensemble_id = null;
		}
	}
	
	public int get_forecastensemble_id() throws SQLException 
	{
		if (forecastensemble_id == null)
		{
			save();
		}
		return forecastensemble_id;
	}
	
	public void save() throws SQLException
	{
		String script = "";
		
		script += "WITH new_forecastensemble AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.forecastensemble (forecast_id, variableposition_id, ensemble_id, measurementunit_id)" + newline;
		script += "		SELECT " + forecast_id + "," + newline;
		script += "			" + variableposition_id + "," + newline;
		script += "			" + ensemble_id + "," + newline;
		script += "			" + measurementunit_id + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.forecastensemble" + newline;
		script += "			WHERE forecast_id = " + forecast_id + newline;
		script += "				AND variableposition_id = " + variableposition_id + newline;
		script += "				AND ensemble_id = " + ensemble_id + newline;
		script += "				AND measurementunit_id = " + measurementunit_id + newline;
		script += "		)" + newline;
		script += "		RETURNING forecastensemble_id" + newline;
		script += ")" + newline;
		script += "SELECT forecastensemble_id" + newline;
		script += "FROM new_forecastensemble" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT forecastensemble_id" + newline;
		script += "FROM wres.forecastensemble" + newline;
		script += "WHERE forecast_id = " + forecast_id + newline;
		script += "		AND variableposition_id = " + variableposition_id + newline;
		script += "		AND ensemble_id = " + ensemble_id + newline;
		script += "		AND measurementunit_id = " + measurementunit_id + ";";
		
		forecastensemble_id = Database.get_result(script, "forecastensemble_id");
	}
}

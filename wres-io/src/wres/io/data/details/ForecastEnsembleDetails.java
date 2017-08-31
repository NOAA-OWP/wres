package wres.io.data.details;

import java.sql.SQLException;

import wres.io.utilities.Database;
import wres.util.Internal;

/**
 * Defines details about an Ensemble linked to a specific forecast
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class ForecastEnsembleDetails {
	private static final String NEWLINE = System.lineSeparator();
	
	private Integer forecastID = null;
	private Integer ensembleID = null;
	private Integer variablePositionID = null;
	private Integer measurementUnitID = null;
	private Integer forecastEnsembleID = null;
	
	/**
	 * Sets the ID of the forecast. The ID of the ForecastEnsemble is invalidated if the 
	 * the ForecastEnsemble's forecast changes
	 * @param forecast_id The ID of the new Forecast
	 */
	public void setForecastID(Integer forecast_id)
	{
		if (this.forecastID != null && !this.forecastID.equals(forecast_id))
		{
			this.forecastEnsembleID = null;
		}
        this.forecastID = forecast_id;
	}
	
	/**
	 * Sets the ID of the Ensemble that the ForecastEnsemble is linked to. The ID of the ForecastEnsemble
	 * is invalidated if the ID of the Ensemble it is linked to changes
	 * @param ensemble_id The ID of the new ensemble
	 */
	public void setEnsembleID(Integer ensemble_id)
	{
		if (this.ensembleID != null && !this.ensembleID.equals(ensemble_id))
		{
			this.forecastEnsembleID = null;
		}
        this.ensembleID = ensemble_id;
	}
	
	/**
	 * Sets the ID of the variable position for the ForecastEnsemble. The ID of the ForecastEnsemble is
	 * invalidated if the ID of the linked VariablePosition changes
	 * @param variableposition_id The ID of the new variable position
	 */
	public void setVariablePositionID(int variableposition_id)
	{
		if (this.variablePositionID != null && this.variablePositionID != variableposition_id)
		{
			this.forecastEnsembleID = null;
		}
        this.variablePositionID = variableposition_id;
	}
	
	/**
	 * Sets the ID of the unit of measurement connected to the ensemble for this forecast. The ID of the ForecastEnsemble
	 * is invalidated if the ID of the linked Measurement Unit changes
	 * @param measurementunit_id The ID of the new unit of measurement
	 */
	public void setMeasurementUnitID(int measurementunit_id)
	{
		if (this.measurementUnitID != null && this.measurementUnitID != measurementunit_id)
		{
			this.forecastEnsembleID = null;
		}
        this.measurementUnitID = measurementunit_id;
	}
	
	/**
	 * @return Returns the ID in the database corresponding to this combination of Forecast Ensemble details 
	 * @throws SQLException Thrown if the value could not be retrieved from the database
	 */
	public int getForecastEnsembleID() throws SQLException 
	{
		if (forecastEnsembleID == null)
		{
			save();
		}
		return forecastEnsembleID;
	}
	
	/**
	 * Updates the ID for the ensemble for the forecast from the database. If it doesn't exist, it is added and the new ID is
	 * saved.
	 * @throws SQLException Thrown if the value could not be loaded from the database
	 */
    private void save() throws SQLException
	{
		String script = "";
		
		script += "WITH new_forecastensemble AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.forecastensemble (forecast_id, variableposition_id, ensemble_id, measurementunit_id)" + NEWLINE;
		script += "		SELECT " + forecastID + "," + NEWLINE;
		script += "			" + variablePositionID + "," + NEWLINE;
		script += "			" + ensembleID + "," + NEWLINE;
		script += "			" + measurementUnitID + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.forecastensemble" + NEWLINE;
		script += "			WHERE forecast_id = " + forecastID + NEWLINE;
		script += "				AND variableposition_id = " + variablePositionID + NEWLINE;
		script += "				AND ensemble_id = " + ensembleID + NEWLINE;
		script += "				AND measurementunit_id = " + measurementUnitID + NEWLINE;
		script += "		)" + NEWLINE;
		script += "		RETURNING forecastensemble_id" + NEWLINE;
		script += ")" + NEWLINE;
		script += "SELECT forecastensemble_id" + NEWLINE;
		script += "FROM new_forecastensemble" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
		script += "SELECT forecastensemble_id" + NEWLINE;
		script += "FROM wres.forecastensemble" + NEWLINE;
		script += "WHERE forecast_id = " + forecastID + NEWLINE;
		script += "		AND variableposition_id = " + variablePositionID + NEWLINE;
		script += "		AND ensemble_id = " + ensembleID + NEWLINE;
		script += "		AND measurementunit_id = " + measurementUnitID + ";";
		
		forecastEnsembleID = Database.getResult(script, "forecastensemble_id");
	}
}

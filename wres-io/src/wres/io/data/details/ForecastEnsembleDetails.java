package wres.io.data.details;

import java.sql.SQLException;
import java.util.HashMap;

import wres.io.utilities.Database;
import wres.util.Internal;

/**
 * Defines details about an Ensemble linked to a specific forecast
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class ForecastEnsembleDetails {
    private final static short FORECASTVALUE_PARTITION_SPAN = 80;

	private static final String NEWLINE = System.lineSeparator();
    private final static HashMap<Integer, String> FORECASTVALUE_PARITION_NAMES = new HashMap<>();
    private static final Object PARTITION_LOCK = new Object();

	private Integer ensembleID = null;
	private Integer variablePositionID = null;
	private Integer measurementUnitID = null;
	private Integer forecastEnsembleID = null;
    private final Integer sourceID;
    private final String initializationDate;

    public ForecastEnsembleDetails(Integer sourceID, String initializationDate)
    {
        this.sourceID = sourceID;
        this.initializationDate = initializationDate;
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
		script += "		INSERT INTO wres.forecastensemble (variableposition_id, ensemble_id, measurementunit_id, initialization_date)" + NEWLINE;
		script += "		SELECT " + variablePositionID + "," + NEWLINE;
		script += "			" + ensembleID + "," + NEWLINE;
		script += "			" + measurementUnitID + "," + NEWLINE;
		script += "         '" + this.initializationDate + "'" + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.forecastensemble" + NEWLINE;
		script += "			WHERE variableposition_id = " + variablePositionID + NEWLINE;
		script += "				AND ensemble_id = " + ensembleID + NEWLINE;
		script += "             AND initialization_date = '" + this.initializationDate + "'" + NEWLINE;
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
		script += "WHERE variableposition_id = " + variablePositionID + NEWLINE;
		script += "		AND ensemble_id = " + ensembleID + NEWLINE;
		script += "     AND initialization_date = '" + this.initializationDate + "'" + NEWLINE;
        script += "		AND measurementunit_id = " + measurementUnitID + ";";
		
		forecastEnsembleID = Database.getResult(script, "forecastensemble_id");
		this.saveForecastSource();
	}

    /**
     * Links the forecast the information about the source of its data in the database
     * @throws SQLException Thrown if the Forecast and its source could not be properly linked
     */
    private void saveForecastSource() throws SQLException
    {
        String script = "";
        script += "INSERT INTO wres.ForecastSource (forecast_id, source_id)" + NEWLINE;
        script += "SELECT " + this.forecastEnsembleID + ", " + this.sourceID + NEWLINE;
        script += "WHERE NOT EXISTS (" + NEWLINE;
        script += "     SELECT 1" + NEWLINE;
        script += "     FROM wres.ForecastSource" + NEWLINE;
        script += "     WHERE forecast_id = " + this.forecastEnsembleID + NEWLINE;
        script += "         AND source_id = " + this.sourceID + NEWLINE;
        script += ");";

        Database.execute(script);
    }

    public static String getForecastValueParitionName(int lead) throws SQLException {
        Integer partitionNumber = lead / ForecastEnsembleDetails.FORECASTVALUE_PARTITION_SPAN;

        String name;

        synchronized (FORECASTVALUE_PARITION_NAMES)
        {
            if (!FORECASTVALUE_PARITION_NAMES.containsKey(partitionNumber))
            {
                int low = (lead / FORECASTVALUE_PARTITION_SPAN) * FORECASTVALUE_PARTITION_SPAN;
                int high = low + FORECASTVALUE_PARTITION_SPAN;
                name = "partitions.ForecastValue_Lead_" + String.valueOf(partitionNumber);

                StringBuilder script = new StringBuilder();
                script.append("CREATE TABLE IF NOT EXISTS ").append(name).append(NEWLINE);
                script.append("(").append(NEWLINE);
                script.append("		CHECK ( lead >= ")
                      .append(String.valueOf(low))
                      .append(" AND lead < ")
                      .append(String.valueOf(high))
                      .append(" )")
                      .append(NEWLINE);
                script.append(") INHERITS (wres.ForecastValue);");

                synchronized (PARTITION_LOCK)
                {
                    Database.execute(script.toString());
                }

                Database.saveIndex(name,
                                   "ForecastValue_Lead_" + String.valueOf(partitionNumber) + "_Lead_idx",
                                   "lead");

                Database.saveIndex(name,
                                   "ForecastValue_Lead_" + String.valueOf(partitionNumber) + "_ForecastEnsemble_idx",
                                   "forecastensemble_id");

                ForecastEnsembleDetails.FORECASTVALUE_PARITION_NAMES.put(partitionNumber, name);
            }
            else
            {
                name = ForecastEnsembleDetails.FORECASTVALUE_PARITION_NAMES.get(partitionNumber);
            }
        }

        return name;
    }
}

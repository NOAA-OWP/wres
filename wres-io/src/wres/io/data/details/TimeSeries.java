package wres.io.data.details;

import java.sql.SQLException;
import java.util.HashMap;

import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;

/**
 * Defines details about a forecasted time series
 * @author Christopher Tubbs
 */
public final class TimeSeries
{
    /**
     * The number of unique lead times contained within a partition within
     * the database for values linked to a forecasted time series
     */
    private static final short FORECASTVALUE_PARTITION_SPAN = 20;

    /**
     * System agnostic newline character used to make created scripts easier to
     * read
     */
	private static final String NEWLINE = System.lineSeparator();

    /**
     * Mapping between the number of a forecast value partition and its name
     */
    private static final HashMap<Integer, String> FORECASTVALUE_PARITION_NAMES =
            new HashMap<>();

    /**
     * The lock used to protect access to the mapping of partition numbers to
     * names
     */
    private static final Object PARTITION_LOCK = new Object();

    /**
     * The ID of the ensemble for the time series. A time series without
     * an ensemble should be indicated as the "default" time series.
     */
	private Integer ensembleID = null;

    /**
     * The ID of the cross section between a variable and its location
     */
	private Integer variablePositionID = null;

    /**
     * The unit of measurement that values for the time series were taken in
     */
	private Integer measurementUnitID = null;

    /**
     * The ID of the time series in the database
     */
	private Integer timeSeriesID = null;

    /**
     * The ID of the initial source of the data for the time series
     */
    private final Integer sourceID;

    /**
     * The string representation of the date and time of when the forecast
     * began. For instance, if a forecasted value for a time series at a lead
     * time of 1 occured at '01-01-2017 13:00:00', the initialization date
     * would be '01-01-2017 12:00:00'.
     */
    private final String initializationDate;

    public TimeSeries( Integer sourceID,
                       String initializationDate )
    {
        this.sourceID = sourceID;
        this.initializationDate = initializationDate;
    }
	
	/**
	 * Sets the ID of the Ensemble that the time series is linked to. The ID of
     * the time series is invalidated if the ID of the Ensemble it is linked
     * to changes
	 * @param ensembleId The ID of the new ensemble
	 */
	public void setEnsembleID(Integer ensembleId)
	{
		if (this.ensembleID != null && !this.ensembleID.equals(ensembleId))
		{
			this.timeSeriesID = null;
		}
        this.ensembleID = ensembleId;
	}
	
	/**
	 * Sets the ID of the relationship between the variable and its location
     * for this time series. The ID of the time series is
	 * invalidated if the ID of the linked Variable location changes
	 * @param variablePositionID The ID of the new variable location
	 */
	public void setVariablePositionID(int variablePositionID)
	{
		if (this.variablePositionID != null && this.variablePositionID != variablePositionID)
		{
			this.timeSeriesID = null;
		}
        this.variablePositionID = variablePositionID;
	}
	
	/**
	 * Sets the ID of the unit of measurement connected to the ensemble for
     * this Time Series. The ID of the Time Series
	 * is invalidated if the ID of the linked Measurement Unit changes
	 * @param measurementUnitID The ID of the new unit of measurement
	 */
	public void setMeasurementUnitID(int measurementUnitID)
	{
		if (this.measurementUnitID != null && this.measurementUnitID != measurementUnitID)
		{
			this.timeSeriesID = null;
		}
        this.measurementUnitID = measurementUnitID;
	}
	
	/**
	 * @return Returns the ID in the database corresponding to this
     * Time Series. If the ID is not present, it is retrieved from the database
	 * @throws SQLException Thrown if the value could not be retrieved from the database
	 */
	public int getTimeSeriesID() throws SQLException
	{
		if (timeSeriesID == null)
		{
			save();
		}
		return timeSeriesID;
	}
	
	/**
	 * Creates or returns the entry in the database representing this time
     * series
	 * @throws SQLException Thrown if successful communication with the database
     * could not be established.
	 */
    private void save() throws SQLException
	{
		String script = "";

		script += "WITH new_timeseries AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.TimeSeries (variableposition_id, ensemble_id, measurementunit_id, initialization_date)" + NEWLINE;
		script += "		SELECT " + variablePositionID + "," + NEWLINE;
		script += "			" + ensembleID + "," + NEWLINE;
		script += "			" + measurementUnitID + "," + NEWLINE;
		script += "         '" + this.initializationDate + "'" + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.TimeSeries TS" + NEWLINE;
		script += "			INNER JOIN wres.ForecastSource FS" + NEWLINE;
		script += "				ON FS.forecast_id = TS.timeseries_id" + NEWLINE;
		script += "			WHERE TS.variableposition_id = " + variablePositionID + NEWLINE;
		script += "				AND TS.ensemble_id = " + ensembleID + NEWLINE;
		script += "             AND TS.initialization_date = '" + this.initializationDate + "'" + NEWLINE;
        script += "				AND TS.measurementunit_id = " + measurementUnitID + NEWLINE;
        script += "             AND FS.source_id = " + this.sourceID + NEWLINE;
        script += "		)" + NEWLINE;
		script += "		RETURNING timeseries_id" + NEWLINE;
        script += ")," + NEWLINE;
        // Only create the forecast source as part of the transaction that has
        // created the timeseries id. This strategy does not apply to NWM data.
        script += "new_forecastsource AS" + NEWLINE;
        script += "(" + NEWLINE;
        script += "     INSERT INTO wres.ForecastSource (forecast_id, source_id)" + NEWLINE;
        script += "          SELECT timeseries_id, " + this.sourceID + NEWLINE;
        script += "          FROM new_timeseries" + NEWLINE;
        script += "          WHERE timeseries_id IS NOT NULL" + NEWLINE;
        // Note: forecastsource.forecast_id is fk to timeseries.timeseries_id
        script += "     RETURNING forecast_id" + NEWLINE;
        script += ")" + NEWLINE;
        script += "SELECT forecast_id AS timeseries_id, TRUE as wasInserted" + NEWLINE;
        script += "FROM new_forecastsource" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
        script += "SELECT timeseries_id, FALSE as wasInserted" + NEWLINE;
		script += "FROM wres.TimeSeries TS" + NEWLINE;
		script += "INNER JOIN wres.ForecastSource FS" + NEWLINE;
		script += "		ON FS.forecast_id = TS.timeseries_id" + NEWLINE;
		script += "WHERE TS.variableposition_id = " + variablePositionID + NEWLINE;
		script += "		AND TS.ensemble_id = " + ensembleID + NEWLINE;
		script += "     AND TS.initialization_date = '" + this.initializationDate + "'" + NEWLINE;
        script += "		AND TS.measurementunit_id = " + measurementUnitID + NEWLINE;
        script += "     AND EXISTS (" + NEWLINE;
        script += "         SELECT 1" + NEWLINE;
        script += "         FROM wres.ForecastSource FS" + NEWLINE;
        script += "         WHERE FS.forecast_id = TS.timeseries_id" + NEWLINE;
        script += "             AND FS.source_id = " + this.sourceID + NEWLINE;
        script += ");";

        timeSeriesID = Database.getResult( script, "timeseries_id" );
    }

    /**
     * Either creates or returns the name of the partition of where values
     * for this timeseries should be saved based on lead time
     * @param lead The lead time of this time series where values of interest
     *             should be saved
     * @return The name of the partition where values for the indicated lead time
     * should be saved.
     * @throws SQLException Thrown if an error occurs when trying to create the
     * partition in the database
     */
    public static String getForecastValueParitionName(int lead) throws SQLException
    {
        Integer partitionNumber = lead / TimeSeries.FORECASTVALUE_PARTITION_SPAN;

        String name;

        synchronized (FORECASTVALUE_PARITION_NAMES)
        {
            if (!FORECASTVALUE_PARITION_NAMES.containsKey(partitionNumber))
            {
                String partitionNumberWord = partitionNumber.toString();

                String highCheck = null;
                String lowCheck = null;

                // Sometimes the lead times are negative, but the dash is not a
                // valid character in a name in sql, so we replace with a word.
                if ( partitionNumber < 0 )
                {
                    partitionNumberWord = "Negative_"
                                          + Math.abs( partitionNumber );
                    lowCheck = "lead > " + (partitionNumber - 1) * FORECASTVALUE_PARTITION_SPAN;
                    highCheck = "lead <= " + partitionNumber * FORECASTVALUE_PARTITION_SPAN;
				}
				else if ( partitionNumber == 0)
                {
                    highCheck = "lead < " + FORECASTVALUE_PARTITION_SPAN;
                    lowCheck = "lead > " + -FORECASTVALUE_PARTITION_SPAN;
                }
                else
                {
                    lowCheck = "lead >= " + partitionNumber * FORECASTVALUE_PARTITION_SPAN;
                    highCheck = "lead < " + (partitionNumber + 1) * FORECASTVALUE_PARTITION_SPAN;
                }

                name = "partitions.ForecastValue_Lead_" + partitionNumberWord;

                ScriptBuilder script = new ScriptBuilder();
                script.addLine("CREATE TABLE IF NOT EXISTS ", name);
                script.addLine("(");
                script.addTab().addLine("CHECK ( ", highCheck, " AND ", lowCheck, " )");
                script.addLine(") INHERITS (wres.ForecastValue);");
                script.addLine("ALTER TABLE ", name, " ALTER COLUMN lead SET STATISTICS 2000;");
                script.addLine("ALTER TABLE ", name, " ALTER COLUMN timeseries_id SET STATISTICS 2000;");
                script.addLine("ALTER TABLE ", name, " OWNER TO wres;");

                synchronized (PARTITION_LOCK)
                {
                    Database.execute(script.toString());
                }


                Database.saveIndex( name,
                                    "ForecastValue_Lead_"
                                    + partitionNumberWord + "_Lead_idx",
                                    "lead" );

                Database.saveIndex(name,
                                   "ForecastValue_Lead_"
								   + partitionNumberWord + "_TimeSeries_idx",
                                   "timeseries_id");

                TimeSeries.FORECASTVALUE_PARITION_NAMES.put( partitionNumber, name);
            }
            else
            {
                name = TimeSeries.FORECASTVALUE_PARITION_NAMES.get( partitionNumber);
            }
        }

        return name;
    }
}

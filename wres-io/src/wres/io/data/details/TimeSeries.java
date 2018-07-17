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
    private static final short TIMESERIESVALUE_PARTITION_SPAN = 20;

    /**
     * System agnostic newline character used to make created scripts easier to
     * read
     */
	private static final String NEWLINE = System.lineSeparator();

    /**
     * Mapping between the number of a forecast value partition and its name
     */
    private static final HashMap<Integer, String> TIMESERIESVALUE_PARITION_NAMES =
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
	private Integer variableFeatureID = null;

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

    public TimeSeries( Integer sourceID, String initializationDate )
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
	 * @param variableFeatureID The ID of the new variable location
	 */
	public void setVariableFeatureID(int variableFeatureID)
	{
		if (this.variableFeatureID != null && this.variableFeatureID != variableFeatureID)
		{
			this.timeSeriesID = null;
		}
        this.variableFeatureID = variableFeatureID;
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
	    ScriptBuilder script = new ScriptBuilder(  );

		script.addLine("WITH new_timeseries AS");
        script.addLine("(");
        script.addTab().addLine("INSERT INTO wres.TimeSeries (variablefeature_id, ensemble_id, measurementunit_id, initialization_date)");
        script.addTab().addLine("SELECT ", this.variableFeatureID, ",");
        script.addTab(  2  ).addLine(this.ensembleID, ",");
		script.addTab(  2  ).addLine(this.measurementUnitID, ",");
		script.addTab(  2  ).addLine("'", this.initializationDate, "'");
		script.addTab().addLine("WHERE NOT EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.TimeSeries TS");
		script.addTab(  2  ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
		script.addTab(   3   ).addLine("ON TSS.timeseries_id = TS.timeseries_id");
		script.addTab(  2  ).addLine("WHERE TS.variablefeature_id = ", this.variableFeatureID);
		script.addTab(   3   ).addLine("AND TS.ensemble_id = ", this.ensembleID);
		script.addTab(   3   ).addLine("AND TS.initialization_date = '", this.initializationDate, "'");
        script.addTab(   3   ).addLine("AND TS.measurementunit_id = ", this.measurementUnitID);
        script.addTab(   3   ).addLine("AND TSS.source_id = ", this.sourceID);
        script.addTab().addLine(")");
		script.addTab().addLine("RETURNING timeseries_id");
        script.addLine("),");
        // Only create the forecast source as part of the transaction that has
        // created the timeseries id. This strategy does not apply to NWM data.
        script.addLine("new_timeseriessource AS");
        script.addLine("(");
        script.addTab().addLine("INSERT INTO wres.TimeSeriesSource (timeseries_id, source_id)");
        script.addTab().addLine("SELECT timeseries_id, ", this.sourceID);
        script.addTab().addLine("FROM new_timeseries");
        script.addTab().addLine("WHERE timeseries_id IS NOT NULL");
        // Note: timeseriessource.timeseries_id is fk to timeseries.timeseries_id
        script.addTab().addLine("RETURNING timeseries_id");
        script.addLine(")");
        script.addLine("SELECT timeseries_id AS timeseries_id, TRUE as wasInserted");
        script.addLine("FROM new_timeseriessource");
		script.addLine();
		script.addLine("UNION");
		script.addLine();
        script.addLine("SELECT TS.timeseries_id, FALSE as wasInserted");
		script.addLine("FROM wres.TimeSeries TS");
		script.addLine("INNER JOIN wres.TimeSeriesSource TSS");
		script.addTab().addLine("ON TSS.timeseries_id = TS.timeseries_id");
		script.addLine("WHERE TS.variablefeature_id = ", this.variableFeatureID);
		script.addTab().addLine("AND TS.ensemble_id = ", this.ensembleID);
		script.addTab().addLine("AND TS.initialization_date = '", this.initializationDate, "'");
        script.addTab().addLine("AND TS.measurementunit_id = ", this.measurementUnitID);
        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.TimeSeriesSource TSS");
        script.addTab(  2  ).addLine("WHERE TSS.timeseries_id = TS.timeseries_id");
        script.addTab(   3   ).addLine("AND TSS.source_id = ", this.sourceID);
        script.addLine(");");

        this.timeSeriesID = script.retrieve( "timeseries_id" );
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
    public static String getTimeSeriesValuePartition( int lead) throws SQLException
    {
        Integer partitionNumber = lead / TimeSeries.TIMESERIESVALUE_PARTITION_SPAN;

        String name;

        synchronized ( TIMESERIESVALUE_PARITION_NAMES )
        {
            if (!TIMESERIESVALUE_PARITION_NAMES.containsKey( partitionNumber))
            {
                String partitionNumberWord = partitionNumber.toString();

                String highCheck;
                String lowCheck;

                // Sometimes the lead times are negative, but the dash is not a
                // valid character in a name in sql, so we replace with a word.
                if ( partitionNumber < 0 )
                {
                    partitionNumberWord = "Negative_"
                                          + Math.abs( partitionNumber );
                    lowCheck = "lead > " + (partitionNumber - 1) * TIMESERIESVALUE_PARTITION_SPAN;
                    highCheck = "lead <= " + partitionNumber * TIMESERIESVALUE_PARTITION_SPAN;
				}
				else if ( partitionNumber == 0)
                {
                    highCheck = "lead < " + TIMESERIESVALUE_PARTITION_SPAN;
                    lowCheck = "lead > " + -TIMESERIESVALUE_PARTITION_SPAN;
                }
                else
                {
                    lowCheck = "lead >= " + partitionNumber * TIMESERIESVALUE_PARTITION_SPAN;
                    highCheck = "lead < " + (partitionNumber + 1) * TIMESERIESVALUE_PARTITION_SPAN;
                }

                name = "partitions.TimeSeriesValue_Lead_" + partitionNumberWord;

                ScriptBuilder script = new ScriptBuilder();
                script.addLine("CREATE TABLE IF NOT EXISTS ", name);
                script.addLine("(");
                script.addTab().addLine("CHECK ( ", highCheck, " AND ", lowCheck, " )");
                script.addLine(") INHERITS (wres.TimeSeriesValue);");
                script.addLine("ALTER TABLE ", name, " ALTER COLUMN lead SET STATISTICS 2000;");
                script.addLine("ALTER TABLE ", name, " ALTER COLUMN timeseries_id SET STATISTICS 2000;");
                script.addLine("ALTER TABLE ", name, " OWNER TO wres;");

                synchronized (PARTITION_LOCK)
                {
                    Database.execute(script.toString());
                }

                Database.saveIndex( name,
                                    "TimeSeriesValue_Lead_"
                                    + partitionNumberWord + "_Lead_idx",
                                    "lead" );

                Database.saveIndex(name,
                                   "TimeSeriesValue_Lead_"
								   + partitionNumberWord + "_TimeSeries_idx",
                                   "timeseries_id");

                TimeSeries.TIMESERIESVALUE_PARITION_NAMES.put( partitionNumber, name);
            }
            else
            {
                name = TimeSeries.TIMESERIESVALUE_PARITION_NAMES.get( partitionNumber);
            }
        }

        return name;
    }
}

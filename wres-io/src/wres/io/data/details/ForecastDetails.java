package wres.io.data.details;

import java.sql.SQLException;
import java.util.HashMap;

import wres.io.data.caching.DataSources;
import wres.io.utilities.Database;
import wres.util.Internal;

/**
 * Important details about a forecast that predicted values for different variables over some span of time
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class ForecastDetails {
	private final static short FORECASTVALUE_PARTITION_SPAN = 80;

	private final static String NEWLINE = System.lineSeparator();
	private final static HashMap<Integer, String> FORECASTVALUE_PARITION_NAMES = new HashMap<>();
	private static final Object PARTITION_LOCK = new Object();
	
	private String sourcePath = null;
	private String forecastDate = null;
	private String hash = null;

	private Integer forecast_id = null;
	private String creationDate = null;
	private String project = "";
	private String type = "";
	private Integer lead = null;
	private Integer projectID = null;

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
	private void saveForecastSource() throws SQLException
    {
        int sourceID = DataSources.getSourceID(sourcePath, getSourceDate(), this.lead, this.hash);

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

	public static String getForecastValueParitionName(int lead) throws SQLException {
		Integer partitionNumber = lead / ForecastDetails.FORECASTVALUE_PARTITION_SPAN;

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

				ForecastDetails.FORECASTVALUE_PARITION_NAMES.put(partitionNumber, name);
			}
			else
			{
				name = ForecastDetails.FORECASTVALUE_PARITION_NAMES.get(partitionNumber);
			}
		}

		return name;
	}
}

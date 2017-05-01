/**
 * 
 */
package data.details;

// TODO: Uncomment once development on source saving/caching resumes
//import data.SourceCache;
import util.Database;

/**
 * Important details about a forecast that predicted values for different variables over some span of time
 * @author Christopher Tubbs
 */
public final class ForecastDetails {
	private final static String newline = System.lineSeparator();
	
	private String sourcePath = null;
	private String forecast_date = null;
	private Integer forecast_id = null;
	
	/**
	 * The path to the file that contains data for the forecast
	 * @param path The path to the forecast file on the file system
	 */
	public ForecastDetails(String path) {
		this.sourcePath = path;
	}
	
	/**
	 * Sets the date of when the forecast was generated
	 * @param forecastDate The value to update the current forecast date with
	 */
	public void setForecastDate(String forecastDate)
	{
		if (this.forecast_date == null || !this.forecast_date.equalsIgnoreCase(forecastDate))
		{
			this.forecast_date = forecastDate;
			forecast_id = null;
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
	 * @throws Exception Thrown if the ID could not be loaded from the database
	 */
	public int getForecastID() throws Exception
	{
		if (forecast_id == null)
		{
			save();
		}
		
		return forecast_id;
	}
	
	/**
	 * Saves the ID of the Forecast to the Forecast Details. The Forecast is added to the
	 * database if it does not currently exist
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
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
		
		forecast_id = Database.getResult(script, "forecast_id");
		
		saveForecastSource();
	}
	
	/**
	 * Links the forecast the information about the source of its data in the database
	 * @throws Exception Thrown if the Forecast and its source could not be properly linked
	 */
	private void saveForecastSource() throws Exception {
        
        // Uncomment when it is time to resume testing on ingest + source linking
        /*
        int sourceID = SourceCache.getSourceID(sourcePath, forecast_date);
                
        // Link the source to the forecast if there isn't one already
        String script = "";
        script += "INSERT INTO wres.ForecastSource (forecast_id, source_id)" + newline;
        script += "SELECT " + this.forecast_id + ", " + sourceID + newline;
        script += "WHERE NOT EXISTS (" + newline;
        script += "     SELECT 1" + newline;
        script += "     FROM wres.ForecastSource" + newline;
        script += "     WHERE forecast_id = " + this.forecast_id + newline;
        script += "         AND source_id = " + sourceID + newline;
        script += ");";
        
        Database.execute(script);*/
	}
}

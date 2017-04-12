/**
 * 
 */
package reading;


/**
 * @author ctubbs
 *
 */
public class SourceReader {
	public SourceReader(String observation_file, String forecast_file)
	{
		this.observation_filename = observation_file;
		this.forecast_filename = forecast_file;
	}
	
	public static BasicSource get_source(String filename) throws Exception
	{
		return SourceFactory.get_source(filename);
	}
	
	public BasicSource get_observation() throws Exception
	{
		observation = SourceFactory.get_source(observation_filename);		
		return observation;
	}
	
	public BasicSource get_forecast() throws Exception
	{
		forecast = SourceFactory.get_source(forecast_filename);		
		return forecast;
	}
	
	protected String get_observation_filename()
	{
		return observation_filename;
	}
	
	protected String get_forecast_filename()
	{
		return forecast_filename;
	}
	
	protected void set_observation_filename(String new_name)
	{
		observation_filename = new_name;
	}
	
	protected void set_forecast_filename(String new_name)
	{
		forecast_filename = new_name;
	}
	
	private String observation_filename;
	private String forecast_filename;
	
	private BasicSource observation;
	private BasicSource forecast;
}

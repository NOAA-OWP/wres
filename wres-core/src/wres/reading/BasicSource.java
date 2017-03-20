/**
 * 
 */
package wres.reading;

/**
 * @author ctubbs
 *
 */
public abstract class BasicSource {
	
	public void save_forecast() throws Exception
	{
		throw new Exception("Forecasts may not be saved using this type of source.");
	}
	
	public void save_observation() throws Exception
	{
		throw new Exception("Observations may not be saved using this type of source.");
	}

	public String get_filename()
	{
		return filename;
	}
	
	protected void set_filename(String name)
	{
		filename = name;
	}
	
	public SourceType get_source_type()
	{
		return source_type;
	}
	
	protected void set_source_type(SourceType type)
	{
		source_type = type;
	}
	
	private String filename = "";
	
	private SourceType source_type = SourceType.UNDEFINED;
}

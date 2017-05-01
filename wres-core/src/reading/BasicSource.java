/**
 * 
 */
package reading;

import java.nio.file.Paths;

/**
 * @author ctubbs
 *
 */
public abstract class BasicSource {
	
	@SuppressWarnings("static-method")
    public void save_forecast() throws Exception
	{
		throw new Exception("Forecasts may not be saved using this type of source.");
	}
	
	@SuppressWarnings("static-method")
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
	
	protected String get_absolute_filename()
	{
		if (absolute_filename == null)
		{
			absolute_filename = Paths.get(get_filename()).toAbsolutePath().toString();
		}
		return absolute_filename;
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
	private String absolute_filename;
	private SourceType source_type = SourceType.UNDEFINED;
}

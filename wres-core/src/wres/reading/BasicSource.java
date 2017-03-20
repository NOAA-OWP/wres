/**
 * 
 */
package wres.reading;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import wres.reading.misc.ASCIISource;
import wres.reading.nws.DatacardSource;

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
	
	public void print()
	{
		System.out.println(toString());
		
		for (BasicSeries series : time_series)
		{
			series.print();
			System.out.println("");
		}		
	}
	
	@Override
 	public String toString()
	{
		String message = "";
		
		message += "Filename:\t";
		message += filename;
		message += System.lineSeparator();
		
		if (time_zone != wres.util.TimeZone.UNDEFINED)
		{
			message += "Time Zone:\t";
			message += time_zone.toString();
			message += System.lineSeparator();
		}
		
		if (!location_id.isEmpty())
		{
			message += "Location:\t";
			message += location_id;
			message += System.lineSeparator();
		}
		
		if (!ensemble_id.isEmpty())
		{
			message += "Ensemble:\t";
			message += ensemble_id;
			message += System.lineSeparator();
		}
		
		if (!variable_id.isEmpty())
		{
			message += "Variable:\t";
			message += variable_id;
			message += System.lineSeparator();
		}
		
		if (source_type != SourceType.UNDEFINED)
		{
			message += "Type:\t";
			message += source_type.toString();
			message += System.lineSeparator();
		}
		
		message += System.lineSeparator();
		
		message += "Series:";
		message += System.lineSeparator();
		
		return message;
	}

	public String get_filename()
	{
		return filename;
	}
	
	public wres.util.TimeZone get_time_zone()
	{
		return time_zone;
	}
	
	protected void set_time_zone(String offset)
	{
		time_zone = wres.util.TimeZone.valueOf(offset);
	}
	
	protected void set_filename(String name)
	{
		filename = name;
	}
	
	public String get_notes()
	{
		return notes;
	}
	
	public void set_notes(String note)
	{
		notes = note;
	}
	
	public SourceType get_source_type()
	{
		return source_type;
	}
	
	protected void set_source_type(SourceType type)
	{
		source_type = type;
	}
	
	public String get_location_id()
	{
		return location_id;
	}
	
	protected void set_location_id(String id)
	{
		location_id = id;
	}
	
	public String get_variable_id()
	{
		return variable_id;
	}
	
	protected void set_variable_id(String id)
	{
		variable_id = id;
	}
	
	public String get_ensemble_id()
	{
		return ensemble_id;
	}
	
	public void set_ensemble_id(String id)
	{
		ensemble_id = id;
	}
	
	public Iterator<BasicSeries> get_series()
	{
		return time_series.iterator();
	}
	
	private String filename = "";
	private wres.util.TimeZone time_zone = wres.util.TimeZone.UNDEFINED;
	
	protected Date get_forecast_date()
	{
		return forecast_date;
	}
	
	protected void set_forecast_date(Date forecasted_date)
	{
		forecast_date = (Date)forecasted_date.clone();
	}
	
	protected int get_model_id()
	{
		return model_id;
	}
	
	protected void set_model_id(int id)
	{
		model_id = id;
	}
	
	protected void set_model_id(String id)
	{
		model_id = Integer.parseInt(id.trim());
	}
	
	private int model_id;
	protected ArrayList<BasicSeries> time_series = new ArrayList<BasicSeries>();
	private Date forecast_date = null;
	private String notes = "";
	private String location_id = "";
	private String variable_id = "";
	private String ensemble_id = "";
	private SourceType source_type = SourceType.UNDEFINED;
}

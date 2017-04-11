/**
 * 
 */
package reading.fews;

import concurrency.Executor;
import concurrency.SQLExecutor;
import config.SystemConfig;
import data.details.EnsembleDetails;
import data.details.FeatureDetails;
import data.details.ForecastDetails;
import data.details.ForecastEnsembleDetails;
import data.details.MeasurementDetails;
import data.details.VariableDetails;
import reading.XMLReader;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamReader;

/**
 * @author Tubbs
 *
 */
public final class PIXMLReader extends XMLReader 
{	
	private static String newline = System.lineSeparator();
	
	private final static Map<String, Double> in_hours = map_time_to_hours();
	
	private static Map<String, Double> map_time_to_hours() 
	{
		Map<String, Double> mapping = new TreeMap<String, Double>();
		
		mapping.put("second", 1/3600.0);
		mapping.put("hour", 1.0);
		mapping.put("day", 24.0);
		mapping.put("minute", 1/60.0);
		
		return mapping;
	}
	
	/**
	 * @param filename
	 */
	public PIXMLReader(String filename) {
		super(filename);
	}
	
	public PIXMLReader(String filename, boolean is_forecast)
	{
		super(filename);
		this.save_forecast = is_forecast;
	}
	
	@Override
	protected void parse_element(XMLStreamReader reader) {
		if (reader.isStartElement())
		{
			String local_name = reader.getLocalName();
			
			if (local_name.equalsIgnoreCase("series"))
			{
				try {
					parse_series(reader);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void parse_series(XMLStreamReader reader) throws Exception
	{
		//	If the current tag is the series tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("series"))
		{
			reader.next();
		}
		
		String local_name = null;
		
		//	Create new metadata records for the new header (inefficient; refactor later)
		current_ensemble = new EnsembleDetails();
		current_location = new FeatureDetails();
		current_measurement = new MeasurementDetails();
		current_forecast = new ForecastDetails();
		current_forecastensemble = new ForecastEnsembleDetails();
		current_variable = new VariableDetails();
		
		//	Loop through every element in a series (header -> entry -> entry -> ... )
		while (reader.hasNext())
		{
			reader.next();
			if (reader.isEndElement() && reader.getLocalName().equalsIgnoreCase("series"))
			{
				break;
			}
			else if (reader.isStartElement())
			{
				local_name = reader.getLocalName();
				if (local_name.equalsIgnoreCase("header"))
				{
					parse_header(reader);
				}
				else if(local_name.equalsIgnoreCase("event"))
				{
					parse_event(reader);
				}
			}
		}
		
		save_entries();
	}
	
	private void parse_event(XMLStreamReader reader) throws Exception
	{
		lead_time = lead_time + time_step;
		Float value = null;
		String time = "";
		String local_name = null;
		
		for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
		{
			local_name = reader.getAttributeLocalName(attribute_index);
			if (local_name.equalsIgnoreCase("value"))
			{
				value = Float.parseFloat(reader.getAttributeValue(attribute_index));
			}
			else if (local_name.equalsIgnoreCase("date"))
			{
				time = reader.getAttributeValue(attribute_index) + time;
			}
			else if (local_name.equalsIgnoreCase("time"))
			{
				time += " " + reader.getAttributeValue(attribute_index);
			}
		}
		
		if (value != current_missing_value)
		{			
			if (save_forecast)
			{
				add_forecast_event(value);
			}
			else
			{
				add_observed_event(time, value);
			}
		}
		
		if (insert_count >= SystemConfig.maximum_inserts())
		{
			save_entries();
		}
	}
	
	private void add_forecast_event(Float forecasted_value) throws SQLException
	{
		if (insert_count > 0)
		{
			current_script += "," + newline;
		}
		else if(insert_count == 0)
		{
			current_script = get_insert_forecast_header();
		}
		
		current_script += "(" + get_forecastensemble_id() + ", " + lead_time + ", " + forecasted_value + ")";
		
		insert_count++;
	}
	
	private void add_observed_event(String observed_time, Float observed_value) throws SQLException
	{
		if (insert_count > 0)
		{
			current_script += "," + newline;
		}
		else
		{
			current_script = get_insert_observation_header();
		}
		
		current_script += "(" + get_variableposition_id() + ", '" + observed_time + "', " + observed_value + ", " + get_measurement_id() + ")";
		
		insert_count++;
	}
	
	private void save_entries()
	{
		if (insert_count > 0)
		{
			insert_count = 0;
			Executor.execute(new SQLExecutor(current_script));
			current_script = null;
		}
	}
	
	private void parse_header(XMLStreamReader reader) throws Exception
	{
		//	If the current tag is the header tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("header"))
		{
			reader.next();
		}
		String local_name = null;
		lead_time = 0;
				
		//	Scrape all pertinent information from the header
		while (reader.hasNext())
		{
			
			if (reader.isEndElement() && reader.getLocalName().equalsIgnoreCase("header"))
			{
				//	Leave the loop when we arrive at the end tag
				break;
			}
			else if (reader.isStartElement())
			{
				local_name = reader.getLocalName();
				if (local_name.equalsIgnoreCase("locationId"))
				{
					//	If we are at the tag for the location id, save it to the location metadata
					current_location.set_lid(tag_value(reader));
				}
				else if (local_name.equalsIgnoreCase("stationName"))
				{
					//	If we are at the tag for the name of the station, save it to the location
					current_location.station_name = tag_value(reader);
				}
				else if(local_name.equalsIgnoreCase("ensembleId"))
				{
					//	If we are at the tag for the name of the ensemble, save it to the ensemble
					current_ensemble.set_ensemble_name(tag_value(reader));
				}
				else if(local_name.equalsIgnoreCase("qualifierId"))
				{
					//	If we are at the tag for the ensemble qualifier, save it to the ensemble
					current_ensemble.qualifier_id = tag_value(reader);
				}
				else if(local_name.equalsIgnoreCase("ensembleMemberIndex"))
				{
					//	If we are at the tag for the ensemble member, save it to the ensemble
					current_ensemble.set_ensemblemember_id(tag_value(reader));
				}
				else if(local_name.equalsIgnoreCase("forecastDate"))
				{
					String date = null;
					String time = null;
					
					for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
					{
						local_name = reader.getAttributeLocalName(attribute_index);

						if (local_name.equalsIgnoreCase("date"))
						{
							date = reader.getAttributeValue(attribute_index);
						}
						else if (local_name.equalsIgnoreCase("time"))
						{
							time = reader.getAttributeValue(attribute_index);
						}
					}
					//	If we are at the tag for the forecast date, save it to the forecast
					current_forecast.set_forecast_date(date + " " + time);
				}
				else if(local_name.equalsIgnoreCase("units"))
				{
					//	If we are at the tag for the units, save it to the measurement units
					current_measurement.set_unit(tag_value(reader));
				}
				else if(local_name.equalsIgnoreCase("missVal"))
				{
					// If we are at the tag for the missing value definition, record it
					current_missing_value = Float.parseFloat(tag_value(reader));
				}
				else if(local_name.equalsIgnoreCase("timeStep"))
				{
					String unit = null;
					Integer multiplier = null;
					for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
					{
						local_name = reader.getAttributeLocalName(attribute_index);

						if (local_name.equalsIgnoreCase("unit"))
						{
							unit = reader.getAttributeValue(attribute_index);
						}
						else if (local_name.equalsIgnoreCase("multiplier"))
						{
							multiplier = Integer.parseInt(reader.getAttributeValue(attribute_index));
						}
					}
					
					time_step = (int) (in_hours.get(unit) * multiplier);
				}
				else if (local_name.equalsIgnoreCase("parameterId"))
				{
					current_variable.set_variable_name(tag_value(reader));
				}
			}
			reader.next();
		}
	}
	
	private int get_ensemble_id() throws SQLException
	{
		return current_ensemble.get_ensemble_id();
	}
		
	private int get_variableposition_id() throws SQLException
	{
		current_location.set_variable_id(get_variable_id());
		return current_location.get_variableposition_id();
	}
	
	private int get_measurement_id() throws SQLException
	{
		return current_measurement.get_measurementunit_id();
	}
	
	private int get_forecast_id() throws SQLException
	{
		return current_forecast.get_forecast_id();
	}
	
	private int get_forecastensemble_id() throws SQLException
	{
		current_forecastensemble.set_ensemble_id(get_ensemble_id());
		current_forecastensemble.set_forecast_id(get_forecast_id());
		current_forecastensemble.set_measurementunit_id(get_measurement_id());
		current_forecastensemble.set_variableposition_id(get_variableposition_id());
		return current_forecastensemble.get_forecastensemble_id();
	}
	
	private int get_variable_id() throws SQLException
	{
		current_variable.measurementunit_id = get_measurement_id();		
		return current_variable.get_variable_id();
	}
	
	private String get_insert_forecast_header()
	{
		String script = "";
		
		script +=	"INSERT INTO wres.forecastvalue(forecastensemble_id, lead, forecasted_value)" + newline;
		script +=	"VALUES ";
		
		return script;
	}
	
	private String get_insert_observation_header()
	{
		String script = "";
		
		script +=	"INSERT INTO wres.observation(variableposition_id, observation_time, observed_value, measurementunit_id)" + newline;
		script +=	"VALUES ";
		
		return script;
	}
	
	private boolean save_forecast = true;
	private EnsembleDetails current_ensemble = null;
	private FeatureDetails current_location = null;
	private MeasurementDetails current_measurement = null;
	private ForecastDetails current_forecast = null;
	private ForecastEnsembleDetails current_forecastensemble = null;
	private VariableDetails current_variable = null;
	private Float current_missing_value = null;
	private Integer time_step = null;
	private String current_script = null;
	private int insert_count = 0;
	private int lead_time = 0;
}

/**
 * 
 */
package reading.fews;

import concurrency.CopyExecutor;
import concurrency.Executor;
import config.SystemConfig;
import data.EnsembleCache;
import data.FeatureCache;
import data.MeasurementCache;
import data.VariableCache;
import data.details.EnsembleDetails;
import data.details.FeatureDetails;
import data.details.ForecastDetails;
import data.details.ForecastEnsembleDetails;
import data.details.MeasurementDetails;
import data.details.VariableDetails;
import reading.XMLReader;
import util.Database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;

import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tubbs
 *
 */
public final class PIXMLReader extends XMLReader 
{	
	private static String newline = System.lineSeparator();
	
	private final static Map<String, Double> hourConversion = mapTimeToHours();
	
	private static Map<String, Double> mapTimeToHours() 
	{
		Map<String, Double> mapping = new TreeMap<String, Double>();
		
		mapping.put("second", 1/3600.0);
		mapping.put("hour", 1.0);
		mapping.put("day", 24.0);
		mapping.put("minute", 1/60.0);
		
		return mapping;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(PIXMLReader.class);

	/**
	 * @param filename
	 */
	public PIXMLReader(String filename) {
		super(filename);
	}
	
	public PIXMLReader(String filename, boolean is_forecast)
	{
		super(filename);
		this.saveForecast = is_forecast;
	}
	
	@Override
	protected void parse_element(XMLStreamReader reader) {
		if (reader.isStartElement())
		{
			String local_name = reader.getLocalName();
			
			if (local_name.equalsIgnoreCase("series"))
			{
				try {
					parseSeries(reader);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void parseSeries(XMLStreamReader reader) throws Exception
	{
		//	If the current tag is the series tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("series"))
		{
			reader.next();
		}
		
		String localName = null;
		
		//	Create new metadata records for the new header (inefficient; refactor later)
		current_ensemble = new EnsembleDetails();
		current_location = new FeatureDetails();
		current_measurement = new MeasurementDetails();
		currentForecast = new ForecastDetails();
		currentForecastEnsemble = new ForecastEnsembleDetails();
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
				localName = reader.getLocalName();
				if (localName.equalsIgnoreCase("header"))
				{
					parseHeader(reader);
				}
				else if(localName.equalsIgnoreCase("event"))
				{
					parseEvent(reader);
				}
			}
		}
		
		saveEntries();
	}
	
	private void parseEvent(XMLStreamReader reader) throws Exception
	{
		leadTime = leadTime + timeStep;
		Float value = null;
		String time = "";
		String localName = null;
		
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			localName = reader.getAttributeLocalName(attributeIndex);
			if (localName.equalsIgnoreCase("value"))
			{
				value = Float.parseFloat(reader.getAttributeValue(attributeIndex));
			}
			else if (localName.equalsIgnoreCase("date"))
			{
				time = reader.getAttributeValue(attributeIndex) + time;
			}
			else if (localName.equalsIgnoreCase("time"))
			{
				time += " " + reader.getAttributeValue(attributeIndex);
			}
		}
		
		if (value != currentMissingIndex)
		{			
			if (saveForecast)
			{
				addForecastEvent(value);
			}
			else
			{
				addObservedEvent(time, value);
			}
		}
		
		if (insertCount >= SystemConfig.instance().get_maximum_copies())
		{
		    LOGGER.debug("Insert count greater than maximum copies, saving.");
			saveEntries();
		}
	}
	
	private void addForecastEvent(Float forecastedValue) throws Exception
	{
		if (insertCount > 0)
		{
			currentScript += newline;
		}
		else if(insertCount == 0)
		{
			currentTableDefinition = getInsertForecastHeader();
			currentScript = "";
		}
		
		currentScript += getForecastEnsembleID() + delimiter + leadTime + delimiter + forecastedValue;
		
		insertCount++;
	}
	
	private void addObservedEvent(String observedTime, Float observedValue) throws Exception
	{
		if (insertCount > 0)
		{
			currentScript += newline;
		}
		else
		{
			currentTableDefinition = getInsertObservationHeader();
			currentScript = "";
		}
		
		currentScript += currentVariablePositionID;
		currentScript += delimiter;
		currentScript += "'" + observedTime + "'";
		currentScript += delimiter;
		currentScript += observedValue;
		currentScript += delimiter;
		currentScript += getMeasurementID();
		
		insertCount++;
	}
	
	private void saveEntries()
	{
		if (insertCount > 0)
		{
			insertCount = 0;
			Executor.execute(new CopyExecutor(currentTableDefinition, currentScript, delimiter));
			//Database.execute(new CopyExecutor(currentTableDefinition, currentScript, delimiter));
			currentScript = null;
		}
	}
	
	private void parseHeader(XMLStreamReader reader) throws Exception
	{
		//	If the current tag is the header tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("header"))
		{
			reader.next();
		}
		String localName = null;
		leadTime = 0;
				
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
				localName = reader.getLocalName();
				if (localName.equalsIgnoreCase("locationId"))
				{
					currentFeatureID = null;
					
					//	If we are at the tag for the location id, save it to the location metadata
					current_location.set_lid(tagValue(reader));
					currentLID = current_location.getKey();// tagValue(reader);
				}
				else if (localName.equalsIgnoreCase("stationName"))
				{
					//	If we are at the tag for the name of the station, save it to the location
					current_location.station_name = tagValue(reader);
					currentStationName = current_location.station_name;// tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleId"))
				{
					currentEnsembleID = null;
					//	If we are at the tag for the name of the ensemble, save it to the ensemble
					current_ensemble.setEnsembleName(tagValue(reader));
					currentEnsembleName = current_ensemble.getKey().itemOne;// tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("qualifierId"))
				{
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble qualifier, save it to the ensemble
					current_ensemble.qualifierID = tagValue(reader);
					currentQualifierID = current_ensemble.qualifierID;// tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleMemberIndex"))
				{
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble member, save it to the ensemble
					current_ensemble.setEnsembleMemberID(tagValue(reader));
					currentEnsembleMemberID = current_ensemble.getKey().itemTwo;// tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("forecastDate"))
				{
					String date = null;
					String time = null;
					
					for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
					{
						localName = reader.getAttributeLocalName(attributeIndex);

						if (localName.equalsIgnoreCase("date"))
						{
							date = reader.getAttributeValue(attributeIndex);
						}
						else if (localName.equalsIgnoreCase("time"))
						{
							time = reader.getAttributeValue(attributeIndex);
						}
					}
					//	If we are at the tag for the forecast date, save it to the forecast
					currentForecast.set_forecast_date(date + " " + time);
				}
				else if(localName.equalsIgnoreCase("units"))
				{
					//	If we are at the tag for the units, save it to the measurement units
					current_measurement.set_unit(tagValue(reader));
					//currentMeasurementUnit = current_measurement.getKey();// tagValue(reader);
					//currentMeasurementUnitID = MeasurementCache.getMeasurementUnitID(currentMeasurementUnit);
				}
				else if(localName.equalsIgnoreCase("missVal"))
				{
					// If we are at the tag for the missing value definition, record it
					currentMissingIndex = Float.parseFloat(tagValue(reader));
				}
				else if(localName.equalsIgnoreCase("timeStep"))
				{
					String unit = null;
					Integer multiplier = null;
					for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
					{
						localName = reader.getAttributeLocalName(attributeIndex);

						if (localName.equalsIgnoreCase("unit"))
						{
							unit = reader.getAttributeValue(attributeIndex);
						}
						else if (localName.equalsIgnoreCase("multiplier"))
						{
							multiplier = Integer.parseInt(reader.getAttributeValue(attributeIndex));
						}
					}
					
					timeStep = (int) (hourConversion.get(unit) * multiplier);
				}
				else if (localName.equalsIgnoreCase("parameterId"))
				{
					current_variable.setVariableName(tagValue(reader));
					currentVariableName = current_variable.getKey();// tagValue(reader);
					//currentVariableID = VariableCache.getVariableID(currentVariableName, currentMeasurementUnitID);
				}
			}
			reader.next();
		}
	}
	
	private int getEnsembleID() throws Exception
	{
		/*if (currentEnsembleID == null)
		{
			currentEnsembleID = EnsembleCache.getEnsembleID(currentEnsembleName, currentEnsembleMemberID, currentQualifierID);
		}
		return currentEnsembleID;*/
		return current_ensemble.get_ensemble_id();
	}
	
	private int getMeasurementID() throws SQLException
	{
		return current_measurement.get_measurementunit_id();
		//return this.currentMeasurementUnitID;
	}
	
	private int getForecastID() throws SQLException
	{
		return currentForecast.get_forecast_id();
	}
	
	private int getVariablePositionID() throws Exception {
		if (currentVariablePositionID == null)
		{
			current_location.set_variable_id(getVariableID());
			currentVariablePositionID = current_location.get_variableposition_id();
			//currentVariablePositionID = FeatureCache.getVariablePositionID(currentLID, currentStationName, getVariableID());
		}
		return currentVariablePositionID;
	}
	
	private int getForecastEnsembleID() throws Exception
	{
		
		if (currentForecastEnsembleID == null)
		{
			currentForecastEnsemble.setEnsembleID(getEnsembleID());
			currentForecastEnsemble.setForecastID(getForecastID());
			currentForecastEnsemble.setMeasurementUnitID(getMeasurementID());
			currentForecastEnsemble.setVariablePositionID(getVariablePositionID());
			currentForecastEnsembleID = currentForecastEnsemble.getForecastEnsembleID();
		}
		return currentForecastEnsembleID;
	}
	
	private int getVariableID() throws Exception
	{		
		/*if (currentVariableID == null)
		{
			this.currentVariableID = VariableCache.getVariableID(currentVariableName, currentMeasurementUnit);
		}*/
		current_variable.measurementunit_id = getMeasurementID();		
		return current_variable.getVariableID();
		//return this.currentVariableID;
	}
	
	private String getInsertForecastHeader()
	{
		return "wres.ForecastValue(forecastensemble_id, lead, forecasted_value)";
	}
	
	private String getInsertObservationHeader()
	{
		return "wres.Observation(variableposition_id, observation_time, observed_value, measurementunit_id)";
	}
	
	private boolean saveForecast = true;
	private EnsembleDetails current_ensemble = null;
	private FeatureDetails current_location = null;
	private MeasurementDetails current_measurement = null;
	private ForecastDetails currentForecast = null;
	private ForecastEnsembleDetails currentForecastEnsemble = null;
	private VariableDetails current_variable = null;
	private Float currentMissingIndex = null;
	private Integer timeStep = null;
	private String currentScript = "";
	private String currentTableDefinition = null;
	private int insertCount = 0;
	private int leadTime = 0;
	private final String delimiter = "|"; 
	
	private Integer currentMeasurementUnitID = null;
	private Integer currentVariableID = null;
	private Integer currentEnsembleID = null;
	private Integer currentFeatureID = null;
	private Integer currentForecastEnsembleID = null;
	private Integer currentVariablePositionID = null;
	
	private String currentQualifierID = null;
	private String currentLID = null;
	private String currentStationName = null;
	private String currentEnsembleName = null;
	private String currentEnsembleMemberID = null;
	private String currentMeasurementUnit = null;
	private String currentVariableName = null;
}

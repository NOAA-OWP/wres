/**
 * 
 */
package reading.fews;

import concurrency.CopyExecutor;
import config.SystemConfig;
import data.EnsembleCache;
import data.FeatureCache;
import data.MeasurementCache;
import data.SourceCache;
import data.VariableCache;
import data.details.ForecastDetails;
import data.details.ForecastEnsembleDetails;
import reading.XMLReader;
import util.Database;
import util.Utilities;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

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
		this.isForecast = is_forecast;
	}
	
	@Override
	protected void parseElement(XMLStreamReader reader) {
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
		
		currentForecast = new ForecastDetails(this.get_filename());
		currentForecastEnsemble = new ForecastEnsembleDetails();
		
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
			if (isForecast)
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
		
		currentScript += FeatureCache.getVariablePositionID(currentLID, currentStationName, getVariableID());
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
			Database.execute(new CopyExecutor(currentTableDefinition, currentScript, delimiter));
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
		creationDate = null;
		creationTime = null;
				
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
					//currentFeatureID = null;
					
					//	If we are at the tag for the location id, save it to the location metadata
					currentLID = tagValue(reader);
				}
				else if (localName.equalsIgnoreCase("stationName"))
				{
					//	If we are at the tag for the name of the station, save it to the location
					currentStationName = tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleId"))
				{
					currentEnsembleID = null;
					//	If we are at the tag for the name of the ensemble, save it to the ensemble
					currentEnsembleName = tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("qualifierId"))
				{
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble qualifier, save it to the ensemble
					//current_ensemble.qualifierID = tag_value(reader);
					currentQualifierID = tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleMemberIndex"))
				{
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble member, save it to the ensemble
					currentEnsembleMemberID = tagValue(reader);
				}
				else if(localName.equalsIgnoreCase("forecastDate"))
				{
					//	If we are at the tag for the forecast date, save it to the forecast
					currentForecast.set_forecast_date(parseDateTime(reader));
				}
				else if(localName.equalsIgnoreCase("units"))
				{
					//	If we are at the tag for the units, save it to the measurement units
					currentMeasurementUnit = tagValue(reader);
					currentMeasurementUnitID = MeasurementCache.getMeasurementUnitID(currentMeasurementUnit);
				}
				else if(localName.equalsIgnoreCase("missVal"))
				{
					// If we are at the tag for the missing value definition, record it
					currentMissingIndex = Float.parseFloat(tagValue(reader));
				}
				else if (localName.equalsIgnoreCase("startDate"))
				{				
					startDate = parseDateTime(reader);
				}
				else if (Utilities.tagIs(reader, endDate))
				{
					endDate = parseDateTime(reader);
				}
				else if (Utilities.tagIs(reader, "creationDate")) {
					creationDate = Utilities.getXMLText(reader);
				}
				else if (Utilities.tagIs(reader, "creationTime")) {
					creationTime = Utilities.getXMLText(reader);
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
					currentVariableName = tagValue(reader);
					currentVariableID = VariableCache.getVariableID(currentVariableName, currentMeasurementUnitID);
				}
			}
			reader.next();
		}
		
		if (isForecast && this.creationDate != null && this.creationTime != null) {
			this.currentForecast.set_forecast_date(this.creationDate + " " + this.creationTime);
		}
	}
	
	@Override
	/**
	 * Create a source for observations and attach it to all recorded values
	 */
	protected void completeParsing() {
		// Uncomment when it is time to restart testing
		/*if (!isForecast) {
			Integer sourceID = null;
			String massSave = "";
			try {			
				sourceID = getSourceID();
				massSave += "INSERT INTO wres.ObservationSource (observation_id, source_id)" + newline;
				massSave += "SELECT O.observation_id, '" + getSourceID() + newline;
				massSave += "FROM wres.Observation O" + newline;
				massSave += "WHERE O.variableposition_id = " + getVariablePositionID() + newline;
				massSave += "	AND O.measurementunit_id = " + getMeasurementID() + newline;
				
				if (startDate != null) {
					massSave += "	AND observation_time >= '" + startDate + "'" + newline;
				}
				
				if (endDate != null) {
					massSave += "	AND observation_time <= '" + endDate + "'" + newline;
				}
				massSave += "	AND NOT EXISTS (" + newline;
				massSave += "		SELECT 1" + newline;
				massSave += "		FROM wres.ObservationSource OS" + newline;
				massSave += "		WHERE OS.observation_id = O.observation_id" + newline;
				massSave += "			AND OS.source_id = " + getSourceID() + newline;
				massSave += "	);";
			} catch (Exception e) {
				System.err.println();
				System.err.println("The observations for this file could not be linked to any sources.");
				System.err.println("The path to the observation file was: " + String.valueOf(this.get_filename()));
				System.err.println("The id of the source was: " + String.valueOf(sourceID));
				System.err.println();
				System.err.println("The script to save the values was:");
				System.err.println(massSave);
				System.err.println();
				e.printStackTrace();
			}
		}*/
	}

	public static String parseDateTime(XMLStreamReader reader) {
		String date = null;
		String time = null;
		String localName = null;
		
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
		
		return date + " " + time;
	}
	
	private int getEnsembleID() throws Exception {
		if (currentEnsembleID == null) {
			currentEnsembleID = EnsembleCache.getEnsembleID(currentEnsembleName, currentEnsembleMemberID, currentQualifierID);
		}
		return currentEnsembleID;
	}
	
	private int getMeasurementID() throws SQLException {
		return this.currentMeasurementUnitID;
	}
	
	private int getForecastID() throws Exception {
		return currentForecast.get_forecast_id();
	}
	
	private int getVariablePositionID() throws Exception {
		if (currentVariablePositionID == null)
		{
			currentVariablePositionID = FeatureCache.getVariablePositionID(currentLID, currentStationName, getVariableID());
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
		if (currentVariableID == null)
		{
			this.currentVariableID = VariableCache.getVariableID(currentVariableName, currentMeasurementUnit);
		}
		return this.currentVariableID;
	}
	
	private int getSourceID() throws Exception
	{
		if (currentSourceID == null)
		{
			String output_time = null;
			if (this.creationDate != null) {
				output_time = this.creationDate;
				if (this.creationTime != null)
				{
					output_time += " " + this.creationTime;
				}
			}
			else {
				output_time = startDate;
			}
			currentSourceID = SourceCache.getSourceID(get_filename(), output_time);
		}
		return currentSourceID;
	}
	
	private String getInsertForecastHeader()
	{
		return "wres.ForecastValue(forecastensemble_id, lead, forecasted_value)";
	}
	
	private String getInsertObservationHeader()
	{
		return "wres.Observation(variableposition_id, observation_time, observed_value, measurementunit_id)";
	}
	
	private String creationDate = null;
	private String creationTime = null;
	private String startDate = null;
	private String endDate = null;
	private boolean isForecast = true;
	private ForecastDetails currentForecast = null;
	private ForecastEnsembleDetails currentForecastEnsemble = null;
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
	private Integer currentForecastEnsembleID = null;
	private Integer currentVariablePositionID = null;
	private Integer currentSourceID = null;
	
	private String currentQualifierID = null;
	private String currentLID = null;
	private String currentStationName = null;
	private String currentEnsembleName = null;
	private String currentEnsembleMemberID = null;
	private String currentMeasurementUnit = null;
	private String currentVariableName = null;
}

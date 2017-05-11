/**
 * 
 */
package reading.fews;

import concurrency.CopyExecutor;
import config.SystemConfig;
import data.caching.EnsembleCache;
import data.caching.FeatureCache;
import data.caching.MeasurementCache;

// TODO: Uncomment when work on the SourceCache continues
//import data.SourceCache;
import data.caching.VariableCache;
import data.details.ForecastDetails;
import data.details.ForecastEnsembleDetails;
import reading.XMLReader;
import util.Database;
import util.Utilities;

import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Christopher Tubbs
 * Loads a PIXML file, iterates through it, and saves all data to the database, whether it is
 * forecast or observation data
 */
public final class PIXMLReader extends XMLReader 
{	
    /**
     * Alias for the system agnostic newline separator
     */
	private final static String newline = System.lineSeparator();

	/**
	 * Message logger for the PIXMLReader class
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(PIXMLReader.class);

	/**
	 * Constructor for a reader for forecast data
	 * @param filename The path to the file to read
	 */
	public PIXMLReader(String filename) {
		super(filename);
	}
	
	/**
	 * Constructor for a reader that may be for forecasts or observations
	 * @param filename The path to the file to read
	 * @param isForecast Whether or not the reader is for forecast data
	 */
	public PIXMLReader(String filename, boolean isForecast)
	{
		super(filename);
		this.isForecast = isForecast;
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

	/**
	 * Interprets information within PIXML "series" tags
	 * @param reader The XML reader, positioned at a "series" tag
	 * @throws Exception Thrown if the XML could not be properly read or interpreted
	 */
	private void parseSeries(XMLStreamReader reader) throws Exception
	{
		//	If the current tag is the series tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("series"))
		{
			reader.next();
		}
		
		String localName = null;
		
		if (isForecast) {
		    currentForecast = new ForecastDetails(this.getFilename());
		    currentForecastEnsemble = new ForecastEnsembleDetails();
		}
		
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
	
	/**
	 * Removes information about a measurement from an "event" tag. If a sufficient number of events have been
	 * parsed, they are sent to the database to be saved.
	 * @param reader The reader containing the current event tag
	 * @throws Exception Any possible error thrown while attempting to read the database
	 */
	private void parseEvent(XMLStreamReader reader) throws Exception
	{
		leadTime = leadTime + timeStep;
		Float value = null;
		String time = "";
		String localName = null;
		
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex) {
			localName = reader.getAttributeLocalName(attributeIndex);
			
			if (localName.equalsIgnoreCase("value")) {
			    
			    // The value is parsed so that we can compare it to the missing value
				value = Float.parseFloat(reader.getAttributeValue(attributeIndex));
			} else if (localName.equalsIgnoreCase("date")) {
				time = reader.getAttributeValue(attributeIndex) + time;
			} else if (localName.equalsIgnoreCase("time")) {
				time += " " + reader.getAttributeValue(attributeIndex);
			}
		}
		
		if (value != currentMissingIndex) {			
			if (isForecast) {
				addForecastEvent(value);
			} else {
				addObservedEvent(time, value);
			}
		}
		
		if (insertCount >= SystemConfig.getMaximumCopies()) {
		    LOGGER.debug("Insert count greater than maximum copies, saving.");
			saveEntries();
		}
	}
	
	/**
	 * Adds measurement information to the current insert script in the form of forecast data
	 * @param forecastedValue The value parsed out of the XML
	 * @throws Exception Any possible error encountered while trying to collect the forecast ensemble ID
	 */
	private void addForecastEvent(Float forecastedValue) throws Exception
	{
		if (insertCount > 0) {
			currentScript += newline;
		} else if(insertCount == 0) {
			currentTableDefinition = getInsertForecastHeader();
			currentScript = "";
		}
		
		currentScript += getForecastEnsembleID() + delimiter + leadTime + delimiter + forecastedValue;
		
		insertCount++;
	}
	
	/**
	 * Adds measurement information to the current insert script in the form of observation data
	 * @param observedTime The time when the measurement was taken
	 * @param observedValue The value retrieved from the XML
	 * @throws Exception Any possible error encountered while trying to retrieve the variable position id or the id of the measurement uni
	 */
	private void addObservedEvent(String observedTime, Float observedValue) throws Exception {
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
	
	/**
	 * Saves the currently built up save script to the database
	 */
	private void saveEntries()
	{
		if (insertCount > 0)
		{
			insertCount = 0;
			Database.execute(new CopyExecutor(currentTableDefinition, currentScript, delimiter));
			currentScript = null;
		}
	}
	
	/**
	 * Interprets the information within PIXML "header" tags
	 * @param reader The XML reader, positioned at a "header" tag
	 * @throws Exception Thrown if the XML could not be read or interpreted properly
	 */
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
					currentLID = Utilities.getXMLText(reader);

                    // TODO: Add FCST to the model
					// LIDs have a length of 5. If it is greater, it is of the form of LID+FCST
					if (currentLID.length() > 5)
					{
					    //currentFCST = currentLID.substring(5, currentLID.length());
					    currentLID = currentLID.substring(0, 5);
					}
				}
				else if (localName.equalsIgnoreCase("stationName"))
				{
					//	If we are at the tag for the name of the station, save it to the location
					currentStationName = Utilities.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleId"))
				{
					currentEnsembleID = null;
					//	If we are at the tag for the name of the ensemble, save it to the ensemble
					currentEnsembleName = Utilities.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("qualifierId"))
				{
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble qualifier, save it to the ensemble
					//current_ensemble.qualifierID = tag_value(reader);
					currentQualifierID = Utilities.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleMemberIndex"))
				{
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble member, save it to the ensemble
					currentEnsembleMemberID = Utilities.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("forecastDate"))
				{
					//	If we are at the tag for the forecast date, save it to the forecast
					currentForecast.setForecastDate(parseDateTime(reader));
				}
				else if(localName.equalsIgnoreCase("units"))
				{
					//	If we are at the tag for the units, save it to the measurement units
					currentMeasurementUnit = Utilities.getXMLText(reader);
					currentMeasurementUnitID = MeasurementCache.getMeasurementUnitID(currentMeasurementUnit);
				}
				else if(localName.equalsIgnoreCase("missVal"))
				{
					// If we are at the tag for the missing value definition, record it
					currentMissingIndex = Float.parseFloat(Utilities.getXMLText(reader));
				}
				// TODO: Uncomment when work on the source saving continues
				/*else if (localName.equalsIgnoreCase("startDate"))
				{				
					startDate = parseDateTime(reader);
				}*/
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
					int multiplier = 1;
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
					
					timeStep = (int) (Utilities.HOUR_CONVERSION.get(unit) * multiplier);
				}
				else if (localName.equalsIgnoreCase("parameterId"))
				{
					currentVariableName = Utilities.getXMLText(reader);
					currentVariableID = VariableCache.getVariableID(currentVariableName, currentMeasurementUnitID);
				}
			}
			reader.next();
		}
		
		if (isForecast && this.creationDate != null && this.creationTime != null) {
			this.currentForecast.setForecastDate(this.creationDate + " " + this.creationTime);
		}
	}
	
	@Override
	/**
	 * Create a source for observations and attach it to all recorded values
	 */
	protected void completeParsing() {
		// TODO: Uncomment when it is time to restart testing
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

	/**
	 * Reads the date and time from an XML reader that stores the date and time in separate attributes
	 * @param reader The XML Reader positioned at a node containing date and time attributes
	 * @return A string of the format "{date} {time}"
	 */
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
	
	/**
	 * @return The ID of the current ensemble
	 * @throws Exception Thrown if the ensemble ID could not be retrieved from the cache
	 */
	private int getEnsembleID() throws Exception {
		if (currentEnsembleID == null) {
			currentEnsembleID = EnsembleCache.getEnsembleID(currentEnsembleName, currentEnsembleMemberID, currentQualifierID);
		}
		return currentEnsembleID;
	}
	
	/**
	 * @return The ID of the unit of measurement that the current variable is being measured in
	 */
	private int getMeasurementID() {
		return this.currentMeasurementUnitID;
	}
	
	/**
	 * @return The ID of the forecast that is currently being parsed
	 * @throws Exception Thrown if the forecast could not be retrieved properly
	 */
	private int getForecastID() throws Exception {
		return currentForecast.getForecastID();
	}
	
	/**
	 * @return The ID of the position for the variable being parsed
	 * @throws Exception Thrown if the ID of the position could not be loaded from the cache
	 */
	private int getVariablePositionID() throws Exception {
		if (currentVariablePositionID == null) {
			currentVariablePositionID = FeatureCache.getVariablePositionID(currentLID, currentStationName, getVariableID());
		}
		return currentVariablePositionID;
	}
	
	/**
	 * @return The ID of the ensemble for the forecast that is tied to a specific variable
	 * @throws Exception Thrown if intermediary values could not be loaded from their own caches or if interaction
	 * with the database failed.
	 */
	private int getForecastEnsembleID() throws Exception {
		if (currentForecastEnsembleID == null) {
			currentForecastEnsemble.setEnsembleID(getEnsembleID());
			currentForecastEnsemble.setForecastID(getForecastID());
			currentForecastEnsemble.setMeasurementUnitID(getMeasurementID());
			currentForecastEnsemble.setVariablePositionID(getVariablePositionID());
			currentForecastEnsembleID = currentForecastEnsemble.getForecastEnsembleID();
		}
		return currentForecastEnsembleID;
	}
	
	/**
	 * @return The ID of the variable currently being measured
	 * @throws Exception Thrown if interaction with the database failed.
	 */
	private int getVariableID() throws Exception
	{		
		if (currentVariableID == null)
		{
			this.currentVariableID = VariableCache.getVariableID(currentVariableName, currentMeasurementUnit);
		}
		return this.currentVariableID;
	}
	
	// TODO: Uncomment when the above source saving is uncommented
	/**
	 * @return A valid ID for the source of this PIXML file from the database
	 * @throws Exception Thrown if an ID could not be retrieved from the database
	 */
	/*private int getSourceID() throws Exception
	{
		if (currentSourceID == null)
		{
			String output_time = null;
			if (this.creationDate != null) {
				output_time = this.creationDate;
				if (this.creationTime != null) {
					output_time += " " + this.creationTime;
				}
			}
			else {
				output_time = startDate;
			}
			currentSourceID = SourceCache.getSourceID(get_filename(), output_time);
		}
		return currentSourceID;
	}*/
	
	/**
	 * @return The String header for the copy statement for forecasts
	 */
	private static String getInsertForecastHeader()
	{
		return "wres.ForecastValue(forecastensemble_id, lead, forecasted_value)";
	}
	
	/**
	 * @return The String header for the copy statement for observations
	 */
	private static String getInsertObservationHeader()
	{
		return "wres.Observation(variableposition_id, observation_time, observed_value, measurementunit_id)";
	}
	
	/**
	 * The date for when the source was created
	 */
	private String creationDate = null;
	
	/**
	 * The time on the date that the source was created
	 */
	private String creationTime = null;
	
	// TODO: Uncomment when source saving work continues
	/**
	 * The date that data in the source first started being captured
	 */
	//private String startDate = null;
	
	/**
	 * The date that data generation/collection ended
	 */
	private String endDate = null;
	
	/**
	 * Indicates whether or not the data is for forecasts. Default is True
	 */
	private boolean isForecast = true;
	
	/**
	 * Basic details about any current forecast
	 */
	private ForecastDetails currentForecast = null;
	
	/**
	 * Basic details about the current ensemble for a current forecast
	 */
	private ForecastEnsembleDetails currentForecastEnsemble = null;
	
	/**
	 * The value which indicates a null or invalid value from the source
	 */
	private Float currentMissingIndex = null;
	
	/**
	 * Indicates the amount of time in hours between measurements
	 */
	private Integer timeStep = null;
	
	/**
	 * The current state of a script that will be sent to the database
	 */
	private String currentScript = "";
	
	/**
	 * The current definition of the table in which the data will be saved
	 */
	private String currentTableDefinition = null;
	
	/**
	 * The number of values that will be inserted in the next sql call
	 */
	private int insertCount = 0;
	
	/**
	 * The current amount of hours into the forecast
	 */
	private int leadTime = 0;
	
	/**
	 * The delimiter between values for copy statements
	 */
	private final String delimiter = "|"; 
	
	/**
	 * The ID for the unit of measurement for the variable that is currently being parsed
	 */
	private Integer currentMeasurementUnitID = null;
	
	/**
	 * The ID for the variable that is currently being parsed
	 */
	private Integer currentVariableID = null;
	
	/**
	 * The ID for the Ensemble that is currently being parsed
	 */
	private Integer currentEnsembleID = null;
	
	/**
	 * The ID for the Ensemble for the forecast that is currently being parsed
	 */
	private Integer currentForecastEnsembleID = null;
	
	/**
	 * The ID for the position for the variable that is currently being parsed
	 */
	private Integer currentVariablePositionID = null;
	
	// TODO: Uncomment when source saving work continues
	/**
	 * The ID for the current source file
	 */
	//private Integer currentSourceID = null;
	
	/**
	 * The qualifier for the current ensemble
	 */
	private String currentQualifierID = null;
	
	/**
	 * The LID for the feature that the forecast/observation is for
	 */
	private String currentLID = null;
	
	/**
	 * The name of the station tied to the feature that the forecast/observation is for
	 */
	private String currentStationName = null;
	
	/**
	 * The name of the ensemble currently being parsed
	 */
	private String currentEnsembleName = null;
	
	/**
	 * The member ID of the current ensemble
	 */
	private String currentEnsembleMemberID = null;
	
	/**
	 * The name of the unit of measurement that the current variable is measured in
	 */
	private String currentMeasurementUnit = null;
	
	/**
	 * The name of the variable whose values are currently being parsed 
	 */
	private String currentVariableName = null;
}

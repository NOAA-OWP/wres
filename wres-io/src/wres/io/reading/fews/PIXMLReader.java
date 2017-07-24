package wres.io.reading.fews;

import wres.io.concurrency.CopyExecutor;
import wres.io.config.SystemSettings;
import wres.io.config.specification.EnsembleSpecification;
import wres.io.config.specification.FeatureSpecification;
import wres.io.config.specification.LocationSpecification;
import wres.io.config.specification.VariableSpecification;
import wres.io.data.caching.*;
import wres.io.data.details.ForecastDetails;
import wres.io.data.details.ForecastEnsembleDetails;
import wres.io.reading.XMLReader;
import wres.io.utilities.Database;
import wres.util.*;

import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Christopher Tubbs
 * Loads a PIXML file, iterates through it, and saves all data to the database, whether it is
 * forecast or observation data
 */
public final class PIXMLReader extends XMLReader 
{
	private static final int PARTITION_HOURS = 80;

    /**
     * Alias for the system agnostic newline separator
     */
	private final static String NEWLINE = System.lineSeparator();

	private final static String INSERT_OBSERVATION_HEADER = "wres.Observation(variableposition_id, " +
																			  "observation_time, " +
																			  "observed_value, " +
																			  "measurementunit_id, " +
																			  "source_id)";

	private static String createForecastValuePartition(Integer leadTime) throws SQLException {

        String partitionHeader = null;
		if (leadTime == null)
		{
			return null;
		}

		int partitionNumber = leadTime / PARTITION_HOURS + 1;

		int low = (leadTime / PARTITION_HOURS) * PARTITION_HOURS;
		int high = low + PARTITION_HOURS;

		partitionHeader = "partitions.ForecastValue_Lead_";
		partitionHeader += String.valueOf(partitionNumber);

		String createScript = "CREATE TABLE IF NOT EXISTS ";
		createScript += partitionHeader;
		createScript += " ( " + NEWLINE;
		createScript += "	CHECK ( lead >= " + String.valueOf(low) +
                " AND lead < " + String.valueOf(high) + " )" + NEWLINE;
		createScript += ") INHERITS (wres.ForecastValue);";

		synchronized (PARTITION_LOCK)
		{
			Database.execute(createScript);
		}

		Database.saveIndex(partitionHeader,
						   "ForecastValue_Lead_" + String.valueOf(partitionNumber) + "_Lead_idx",
						   "lead" );

		Database.saveIndex(partitionHeader,
						   "ForecastValue_Lead_" + String.valueOf(partitionNumber) + "_ForecastEnsemble_idx",
						   "forecastensemble_id");

		partitionHeader += " (forecastensemble_id, lead, forecasted_value)";
		return partitionHeader;
	}

	private static final Object PARTITION_LOCK = new Object();

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

	public PIXMLReader (String filename, InputStream inputStream)
	{
		super(filename, inputStream);
	}

	public PIXMLReader(String filename, InputStream inputStream, boolean isForecast)
	{
		super(filename, inputStream);
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
		
		String localName;
		
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
					if (!seriesIsApproved()) {
					    XML.skipToEndTag(reader, "series");
					    break;
					}
				}
				else if(localName.equalsIgnoreCase("event"))
				{
					parseEvent(reader);
				}
			}
		}
		
		saveLeftoverObservations();
	}
	
	/**
	 * Removes information about a measurement from an "event" tag. If a sufficient number of events have been
	 * parsed, they are sent to the database to be saved.
	 * @param reader The reader containing the current event tag
	 * @throws Exception Any possible error thrown while attempting to read the database
	 */
	private void parseEvent(XMLStreamReader reader) throws Exception
	{
		this.incrementLead();
		Float value = null;
		StringBuilder time = new StringBuilder();
		String localName;
		
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex) {
			localName = reader.getAttributeLocalName(attributeIndex);
			
			if (localName.equalsIgnoreCase("value")) {
			    
			    // The value is parsed so that we can compare it to the missing value
				value = Float.parseFloat(reader.getAttributeValue(attributeIndex));
			} else if (localName.equalsIgnoreCase("date")) {
				time.insert(0, reader.getAttributeValue(attributeIndex));
			} else if (localName.equalsIgnoreCase("time")) {
				time.append(" ").append(reader.getAttributeValue(attributeIndex));
			}
		}
		
		if (value != null && !value.equals(currentMissingValue)) {
			if (isForecast && dateIsApproved(time.toString()) && valueIsApproved(value)) {
				addForecastEvent(value, currentLeadTime, getForecastEnsembleID());
			} else {
				addObservedEvent(time.toString(), value);
			}
		}
		
		if (insertCount >= SystemSettings.getMaximumCopies()) {
            saveLeftoverObservations();
		}
	}

	/**
	 * Adds measurement information to the current insert script in the form of forecast data
	 * @param forecastedValue The value parsed out of the XML
	 * @throws Exception Any possible error encountered while trying to collect the forecast ensemble ID
	 */
	private static void addForecastEvent(Float forecastedValue, Integer lead, Integer forecastEnsembleID) throws Exception
	{
		synchronized (groupLock) {
            PIXMLReader.getBuilder(lead)
                    .append(forecastEnsembleID)
                    .append(delimiter)
                    .append(lead)
                    .append(delimiter)
                    .append(forecastedValue)
                    .append(NEWLINE);

            copyCount.put(lead, copyCount.get(lead) + 1);

            if (copyCount.get(lead) >= SystemSettings.getMaximumCopies()) {
                CopyExecutor copier = new CopyExecutor(getForecastInsertHeader(lead),
                        getBuilder(lead).toString(),
                        delimiter);
                copier.setOnRun(ProgressMonitor.onThreadStartHandler());
                copier.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                Database.storeIngestTask(Database.execute(copier));

                copyCount.put(lead, 0);
                builderMap.put(lead, new StringBuilder());
            }
        }
	}
	
	/**
	 * Adds measurement information to the current insert script in the form of observation data
	 * @param observedTime The time when the measurement was taken
	 * @param observedValue The value retrieved from the XML
	 * @throws Exception Any possible error encountered while trying to retrieve the variable position id or the id of the measurement uni
	 */
	private void addObservedEvent(String observedTime, Float observedValue) throws Exception {
        if (!dateIsApproved(observedTime) || !valueIsApproved(observedValue)) {
            return;
        }
        
		if (insertCount > 0)
		{
			currentScript.append(NEWLINE);
		}
		else
		{
			currentTableDefinition = INSERT_OBSERVATION_HEADER;
			currentScript = new StringBuilder();
		}
		
		currentScript.append(Features.getVariablePositionID(currentLID, currentStationName, getVariableID()));
		currentScript.append(delimiter);
		currentScript.append("'").append(observedTime).append("'");
		currentScript.append(delimiter);
		currentScript.append(observedValue);
		currentScript.append(delimiter);
		currentScript.append(String.valueOf(getMeasurementID()));
		currentScript.append(delimiter);
		currentScript.append(String.valueOf(getSourceID()));
		
		insertCount++;
	}

	public static void saveLeftoverForecasts()
    {
        synchronized (groupLock)
        {
            for (Map.Entry<Integer, StringBuilder> builderPair : PIXMLReader.builderMap.entrySet()) {
                if (PIXMLReader.copyCount.get(builderPair.getKey()) > 0) {
                    try {
                        String header = PIXMLReader.getForecastInsertHeader(builderPair.getKey());
                        CopyExecutor copier = new CopyExecutor(header, builderPair.getValue().toString(), delimiter);
                        copier.setOnRun(ProgressMonitor.onThreadStartHandler());
                        copier.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                        Database.storeIngestTask(Database.execute(copier));
                        PIXMLReader.builderMap.put(builderPair.getKey(), new StringBuilder());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void saveLeftoverObservations()
    {
        if (insertCount > 0)
        {
            insertCount = 0;
            CopyExecutor copier = new CopyExecutor(currentTableDefinition, currentScript.toString(), delimiter);
            copier.setOnRun(ProgressMonitor.onThreadStartHandler());
            copier.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            Database.storeIngestTask(Database.execute(copier));
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
		String localName;
		currentLeadTime = 0;
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
					//	If we are at the tag for the location id, save it to the location metadata
					currentLID = XML.getXMLText(reader);

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
					currentStationName = XML.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleId"))
				{
				    currentForecastEnsembleID = null;
					currentEnsembleID = null;
					//	If we are at the tag for the name of the ensemble, save it to the ensemble
					currentEnsembleName = XML.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("qualifierId"))
				{
				    currentForecastEnsembleID = null;
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble qualifier, save it to the ensemble
					//current_ensemble.qualifierID = tag_value(reader);
					currentQualifierID = XML.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("ensembleMemberIndex"))
				{
				    currentForecastEnsembleID = null;
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble member, save it to the ensemble
					currentEnsembleMemberID = XML.getXMLText(reader);
				}
				else if(localName.equalsIgnoreCase("forecastDate"))
				{
					//	If we are at the tag for the forecast date, save it to the forecast
					currentForecast.setForecastDate(parseDateTime(reader));
				}
				else if(localName.equalsIgnoreCase("units"))
				{
					//	If we are at the tag for the units, save it to the measurement units
					currentMeasurementUnit = XML.getXMLText(reader);
					currentMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(currentMeasurementUnit);
				}
				else if(localName.equalsIgnoreCase("missVal"))
				{
					// If we are at the tag for the missing value definition, record it
					currentMissingValue = Float.parseFloat(XML.getXMLText(reader));
				}
				// TODO: Uncomment when work on the source saving continues
				else if (localName.equalsIgnoreCase("startDate"))
				{				
					startDate = parseDateTime(reader);
				}
				else if (XML.tagIs(reader, endDate))
				{
					endDate = parseDateTime(reader);
				}
				else if (XML.tagIs(reader, "creationDate")) {
					creationDate = XML.getXMLText(reader);
				}
				else if (XML.tagIs(reader, "creationTime")) {
					creationTime = XML.getXMLText(reader);
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
					
					timeStep = Time.unitsToHours(unit, multiplier).intValue();
				}
				else if (localName.equalsIgnoreCase("parameterId"))
				{
					currentVariableName = XML.getXMLText(reader);
					currentVariableID = Variables.getVariableID(currentVariableName, currentMeasurementUnitID);
				}
			}
			reader.next();
		}
	
		if (isForecast && this.creationDate != null && this.creationTime != null) {
			this.currentForecast.setCreationDate(this.creationDate + " " + this.creationTime);
		}
	}

	/**
	 * Reads the date and time from an XML reader that stores the date and time in separate attributes
	 * @param reader The XML Reader positioned at a node containing date and time attributes
	 * @return A string of the format "{date} {time}"
	 */
	private static String parseDateTime (XMLStreamReader reader) {
		String date = null;
		String time = null;
		String localName;
		
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
			currentEnsembleID = Ensembles.getEnsembleID(currentEnsembleName, currentEnsembleMemberID, currentQualifierID);
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
			currentVariablePositionID = Features.getVariablePositionID(currentLID, currentStationName, getVariableID());
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
			this.currentVariableID = Variables.getVariableID(currentVariableName, currentMeasurementUnit);
		}
		return this.currentVariableID;
	}
	
	/**
	 * @return A valid ID for the source of this PIXML file from the database
	 * @throws Exception Thrown if an ID could not be retrieved from the database
	 */
	private int getSourceID() throws Exception
	{
		if (currentSourceID == null)
		{
			String output_time;
			if (this.creationDate != null) {
				output_time = this.creationDate;
				if (this.creationTime != null) {
					output_time += " " + this.creationTime;
				}
			}
			else {
				output_time = startDate;
			}
			currentSourceID = DataSources.getSourceID(getFilename(), output_time, null);
		}
		return currentSourceID;
	}
	
	public void setSpecifiedEarliestDate(String earliestDate) {
	    this.specifiedEarliestDate = Time.convertStringToDate(earliestDate);
	    this.detailsSpecified = true;
	}
	
	public void setSpecifiedLatestDate(String latestDate) {
	    this.specifiedLatestDate = Time.convertStringToDate(latestDate);
	    this.detailsSpecified = true;
	}
	
	public void setSpecifiedMinimumValue(String minimumValue) {
	    if (Strings.isNumeric(minimumValue)) {
	        this.specifiedMinimumValue = Float.parseFloat(minimumValue);
	        this.detailsSpecified = true;
	    }
	}
	
	public void setSpecifiedMaximumValue(String maximumValue) {
	    if (Strings.isNumeric(maximumValue)) {
	        this.specifiedMaximumValue = Float.parseFloat(maximumValue);
	        this.detailsSpecified = true;
	    }
	}
	
	public void setSpecifiedVariables(List<VariableSpecification> variables) {
	    this.specifiedVariables = variables;
	    this.loadAllVariables = this.specifiedVariables == null || this.specifiedVariables.size() == 0;
	    if (!loadAllVariables) {
	        this.detailsSpecified = true;
	    }
	}
	
	public void setSpecifiedEnsembles(List<EnsembleSpecification> ensembles) {
	    this.specifiedEnsemblesToLoad = ensembles;
	    this.loadAllEnsembles = this.specifiedEnsemblesToLoad == null || this.specifiedEnsemblesToLoad.size() == 0;
	    if (!loadAllEnsembles) {
	        this.detailsSpecified = true;
	    }
	}
	
	public void setSpecifiedFeatures(List<FeatureSpecification> features) {
	    this.specifiedFeatures = features;
	    this.loadAllFeatures = this.specifiedFeatures == null || this.specifiedFeatures.size() == 0;
	    if (!loadAllFeatures) {
	        this.detailsSpecified = true;
	    }
	}
	
	private boolean dateIsApproved(String date) {
	    if (!detailsSpecified || (this.specifiedEarliestDate == null && this.specifiedLatestDate == null)) {
	        return true;
	    }
	    OffsetDateTime dateToApprove = Time.convertStringToDate(date);
	    return dateToApprove.isAfter(specifiedEarliestDate) && dateToApprove.isBefore(specifiedLatestDate);
	}
	
	private boolean valueIsApproved(Float value) {
	    return !value.equals(this.currentMissingValue) &&
	           value >= this.specifiedMinimumValue && 
	           value <= this.specifiedMaximumValue; 
	}
    
    private boolean variableIsApproved (String name) {
        return !this.detailsSpecified || 
               this.loadAllVariables || 
               this.specifiedVariables == null ||
               this.specifiedVariables.size() == 0 ||
               Collections.exists(this.specifiedVariables, (VariableSpecification specification) -> {
                   return specification.name().equalsIgnoreCase(name);
        });
    }
    
    private boolean ensembleIsApproved (String name, String ensembleMemberID) {
        return !isForecast ||
               !detailsSpecified || 
               this.loadAllEnsembles || 
               this.specifiedEnsemblesToLoad == null ||
               this.specifiedEnsemblesToLoad.size() == 0 ||
               Collections.exists(this.specifiedEnsemblesToLoad, (EnsembleSpecification specification) -> {
                   return specification.getName().equalsIgnoreCase(name) && specification.getMemberID().equalsIgnoreCase(ensembleMemberID);
        });
    }
    
    private boolean featureIsApproved (String lid) {
        return !detailsSpecified || 
               this.loadAllFeatures ||
               this.specifiedFeatures == null ||
               this.specifiedFeatures.size() == 0 ||
               Collections.exists(this.specifiedFeatures, (FeatureSpecification specification) -> {
                   return specification.isLocation() && ((LocationSpecification)specification).lid().equalsIgnoreCase(lid);
               });
    }
    
    private boolean seriesIsApproved () {
        boolean ensembleApproved = this.ensembleIsApproved(this.currentEnsembleName, this.currentEnsembleMemberID);
        boolean featureApproved = this.featureIsApproved(this.currentLID);
        boolean variableApproved = this.variableIsApproved(currentVariableName);
        
        return featureApproved && variableApproved && ensembleApproved;
    }
	
	/**
	 * The date for when the source was created
	 */
	private String creationDate = null;
	
	/**
	 * The time on the date that the source was created
	 */
	private String creationTime = null;

	/**
	 * The date that data in the source first started being captured
	 */
	private String startDate = null;
	
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
	private Float currentMissingValue = null;
	
	/**
	 * Indicates the amount of time in hours between measurements
	 */
	private Integer timeStep = null;
	
	/**
	 * The current state of a script that will be sent to the database
	 */
	private StringBuilder currentScript = null;
	
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
	private int currentLeadTime = 0;
	
	/**
	 * The delimiter between values for copy statements
	 */
	private static final String delimiter = "|";
	
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

	/**
	 * The ID for the current source file
	 */
	private Integer currentSourceID = null;
	
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

	private void incrementLead() {
		this.currentLeadTime += this.timeStep;
	}

	private static StringBuilder getBuilder(Integer lead)
    {
        synchronized (groupLock)
        {
            if (!builderMap.containsKey(lead))
            {
                builderMap.putIfAbsent(lead, new StringBuilder());
                copyCount.putIfAbsent(lead, 0);
            }
            return builderMap.get(lead);
        }
    }

	private String getForecastInsertHeader() throws SQLException {
	    return getForecastInsertHeader(this.currentLeadTime);
	}

	private static String getForecastInsertHeader(Integer lead) throws SQLException {
	    synchronized (PIXMLReader.headerMap) {
            if (!PIXMLReader.headerMap.containsKey(lead)) {
                PIXMLReader.headerMap.putIfAbsent(lead, PIXMLReader.createForecastValuePartition(lead));
            }
        }
	    return PIXMLReader.headerMap.get(lead);
    }

    private OffsetDateTime specifiedEarliestDate = null;
    private OffsetDateTime specifiedLatestDate = null;
    private Float specifiedMinimumValue = Float.MIN_VALUE;
    private Float specifiedMaximumValue = Float.MAX_VALUE;
    private List<VariableSpecification> specifiedVariables = null;
    private boolean loadAllVariables = true;
    private boolean loadAllEnsembles = true;
    private List<EnsembleSpecification> specifiedEnsemblesToLoad = null;
    private boolean loadAllFeatures = true;
    private List<FeatureSpecification> specifiedFeatures = null;
    private boolean detailsSpecified = false;

    private static final Map<Integer, StringBuilder> builderMap = new TreeMap<>();
    private static final Map<Integer, String> headerMap = new TreeMap<>();
    private static final Map<Integer, Integer> copyCount = new TreeMap<>();
    private static final Object groupLock = new Object();
}

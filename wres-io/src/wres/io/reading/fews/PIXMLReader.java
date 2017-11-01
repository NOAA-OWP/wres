package wres.io.reading.fews;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.concurrency.CopyExecutor;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.TimeSeries;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.IngestException;
import wres.io.reading.InvalidInputDataException;
import wres.io.reading.XMLReader;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.Internal;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.Time;
import wres.util.XML;

/**
 * @author Christopher Tubbs
 * Loads a PIXML file, iterates through it, and saves all data to the database, whether it is
 * forecast or observation data
 */
@Internal(exclusivePackage = "wres.io")
public final class PIXMLReader extends XMLReader 
{
	private static final Logger LOGGER = LoggerFactory.getLogger(PIXMLReader.class);

    /**
     * Epsilon value used to test floating point equivalency
     */
    private static final double EPSILON = 0.0000001;

    /**
     * Alias for the system agnostic newline separator
     */
	private final static String NEWLINE = System.lineSeparator();

	private final static String INSERT_OBSERVATION_HEADER = "wres.Observation(variableposition_id, " +
																			  "observation_time, " +
																			  "observed_value, " +
																			  "measurementunit_id, " +
																			  "source_id)";

    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter FORMATTER
            = DateTimeFormatter.ofPattern( PATTERN,
                                           Locale.US )
                               .withZone( ZoneId.of( "UTC" ) );

    private DataSourceConfig.Source sourceConfig;

	private static String createForecastValuePartition(Integer leadTime) throws SQLException {

		if (leadTime == null)
		{
			return null;
		}
		String partitionHeader = TimeSeries.getForecastValueParitionName( leadTime);

		partitionHeader += " (timeseries_id, lead, forecasted_value)";
		return partitionHeader;
	}

	/**
	 * Constructor for a reader that may be for forecasts or observations
	 * @param filename The path to the file to read
	 * @param isForecast Whether or not the reader is for forecast data
	 */
    @Internal(exclusivePackage = "wres.io")
	public PIXMLReader(String filename, boolean isForecast, String hash, ProjectDetails projectDetails)
	{
		super(filename);
		this.isForecast = isForecast;
		this.hash = hash;
		this.projectDetails = projectDetails;
	}

    @Internal(exclusivePackage = "wres.io")
	public PIXMLReader(String filename, InputStream inputStream, boolean isForecast, String hash, ProjectDetails projectDetails)
	{
		super(filename, inputStream);
		this.isForecast = isForecast;
		this.hash = hash;
		this.projectDetails = projectDetails;
	}

	@Override
	protected void parseElement( XMLStreamReader reader )
			throws IOException
	{
		if (reader.isStartElement())
		{
            String localName = reader.getLocalName();

            if ( localName.equalsIgnoreCase( "timeZone" ) )
            {
                try
                {
                    parseOffsetHours( reader );
                }
                catch ( XMLStreamException e )
                {
                    String message = "While reading the time zone at line "
                                     + reader.getLocation().getLineNumber()
                                     + " and column "
                                     + reader.getLocation().getColumnNumber()
                                     + ", encountered an issue.";
                    throw new IOException( message, e );
                }
            }
            else if ( localName.equalsIgnoreCase( "series" ) )
            {
                try
                {
                    parseSeries( reader );
                }
                catch ( XMLStreamException
                        | SQLException
                        | ProjectConfigException e )
                {
                    String message = "While ingesting timeseries at line "
                                     + reader.getLocation().getLineNumber()
                                     + " and column "
                                     + reader.getLocation().getColumnNumber()
                                     + ", encountered an issue.";
                    throw new IngestException( message, e );
                }
			}
		}
	}

    @Override
    protected void completeParsing()
            throws IOException
    {
        if ( this.hash != null && !this.hash.trim().isEmpty() )
        {
            try
			{
				this.projectDetails.addSource( this.hash,
											   this.dataSourceConfig );
			}
            catch ( SQLException se )
            {
                String message = "Could not save data source information for "
                                 + this.getFilename();
                throw new IngestException( message, se );
            }
        }
        else
        {
            LOGGER.debug( "No data could be ingested from '{}'.",
                          this.getFilename());
        }
    }

    /**
     * Parses offset hours from a reader that is positioned on "timeZone" tag.
     * <br />
     * Sets this reader's offset hours after parsing the value.
     * @param reader the reader positioned on the timeZone tag
     * @throws XMLStreamException when underlying reader can't read next element
     * @throws NumberFormatException when value is not able to be parsed
     * @throws DateTimeException when the value is outside the range +/- 18 hrs
     */
    private void parseOffsetHours( XMLStreamReader reader )
            throws XMLStreamException
    {
        if ( reader.isStartElement()
             && reader.getLocalName().equalsIgnoreCase("timeZone"))
        {
            reader.next();
        }
        String offsetValueText = reader.getText();
        double offsetHours = Double.parseDouble( offsetValueText );
        // There are timezones such as +8:45
        int offsetSeconds = (int) ( offsetHours * 3600.0 );
        this.zoneOffset = ZoneOffset.ofTotalSeconds( offsetSeconds );
    }


    /**
	 * Interprets information within PIXML "series" tags
	 * @param reader The XML reader, positioned at a "series" tag
	 */
	private void parseSeries(XMLStreamReader reader)
            throws XMLStreamException, SQLException, IOException,
            ProjectConfigException
    {
        // If we get to this point without a zone offset, something is wrong.
        // See #38801 discussion.
        if ( this.getZoneOffset() == null )
        {
            String message = "At the point of reading PI-XML series, could not "
                             + "find the zone offset. Have read up to line "
                             + reader.getLocation().getLineNumber()
                             + " and column "
                             + reader.getLocation().getColumnNumber() + ". "
                             + "In PI-XML data, a field named <timeZone> "
                             + "containing the number of hours to offset from "
                             + "UTC is required for reliable ingest. Please "
                             + "report this issue to whomever provided this "
                             + "data file.";
            throw new InvalidInputDataException( message );
        }
        else
        {
            ZoneOffset configuredOffset
                = ConfigHelper.getZoneOffset( this.getSourceConfig() );
            if ( configuredOffset != null
                 && !configuredOffset.equals( this.getZoneOffset() ) )
            {
                String message =
                        "The zone offset specified for a PI-XML source ("
                        + configuredOffset.toString()
                        + ") did not match what was in the source data ("
                        + this.getZoneOffset().toString()
                        + "). It is best to NOT specify the zone for PI-XML "
                        + "sources in the project configuration because WRES "
                        + "can simply use the zone offset found in the data.";
                throw new ProjectConfigException( this.getSourceConfig(),
                                                  message );
            }
        }

		//	If the current tag is the series tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("series"))
		{
			reader.next();
		}

		this.currentTimeSeries = null;

		String localName;

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
					    LOGGER.debug( "The encountered time series is not approved by the specifications. Moving on." );
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
     * @throws SQLException when unable to get a series id or unable to save
     * @throws ProjectConfigException when a forecast is missing a forecast date
	 */
	private void parseEvent(XMLStreamReader reader)
            throws SQLException, ProjectConfigException
    {
		this.incrementLead();
		String value = null;
		String localName;
        LocalDate localDate = null;
        LocalTime localTime = null;
        String dateText = "";
        String timeText = "";

        // TODO: Return lead logic to the programmatic method, not the date time
        // object allocation method. The date time allocation method doubles
        // the processing time for small data sets and increases
        // super linearly and increases the memory footprint.
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			localName = reader.getAttributeLocalName(attributeIndex);
			
			if (localName.equalsIgnoreCase("value"))
			{
				value = reader.getAttributeValue(attributeIndex);
			}
			else if (localName.equalsIgnoreCase("date"))
			{
                dateText = reader.getAttributeValue(attributeIndex);
                localDate = LocalDate.parse( dateText );
			}
			else if (localName.equalsIgnoreCase("time"))
			{
                timeText = reader.getAttributeValue(attributeIndex);
                localTime = LocalTime.parse( timeText );
			}
		}

        LocalDateTime dateTime = LocalDateTime.of( localDate, localTime );
        if (isForecast)
        {
            if ( this.getForecastDate() == null )
            {
                String message = "No forecast date found for forecast data."
                                 + " Might this really be observation data?"
                                 + " Please check the <type> under both "
                                 + "<left> and <right> sources.";
                throw new ProjectConfigException( this.getDataSourceConfig(),
                                                  message );
            }
            Duration leadTime = Duration.between( this.getForecastDate(),
                                                  dateTime );
            int leadTimeInHours = (int) leadTime.toHours();

            PIXMLReader.addForecastEvent( this.getValueToSave( value ),
                                          leadTimeInHours,
                                          getTimeSeriesID() );

        }
        else
        {
            if (value.trim().equalsIgnoreCase( "nan" ))
            {
                LOGGER.debug( "NaN encountered." );
            }
            OffsetDateTime offsetDateTime
                = OffsetDateTime.of( dateTime, this.getZoneOffset() );
            String formattedDate = offsetDateTime.format( FORMATTER );
            addObservedEvent( formattedDate, this.getValueToSave( value ) );
        }
		
		if (insertCount >= SystemSettings.getMaximumCopies()) {
            saveLeftoverObservations();
		}
	}

	/**
	 * Adds measurement information to the current insert script in the form of forecast data
	 * @param forecastedValue The value parsed out of the XML
	 * @throws SQLException Any possible error encountered while trying to collect the forecast ensemble ID
	 */
	private static void addForecastEvent(String forecastedValue,
                                         Integer lead,
                                         Integer timeSeriesID)
            throws SQLException
    {
		synchronized (groupLock)
        {
            PIXMLReader.getBuilder(lead)
                    .append(timeSeriesID)
                    .append(delimiter)
                    .append(lead)
                    .append(delimiter)
                    .append(forecastedValue)
                    .append(NEWLINE);

            copyCount.put(lead, copyCount.get(lead) + 1);

            if (copyCount.get(lead) >= SystemSettings.getMaximumCopies())
            {
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
	 * @throws SQLException Any possible error encountered while trying to retrieve the variable position id or the id of the measurement uni
	 */
	private void addObservedEvent(String observedTime, String observedValue)
            throws SQLException
    {
		if (insertCount > 0)
		{
			currentScript.append(NEWLINE);
		}
		else
		{
			currentTableDefinition = INSERT_OBSERVATION_HEADER;
			currentScript = new StringBuilder();
		}
		
		currentScript.append(this.getVariablePositionID());
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
            for ( Map.Entry<Integer, StringBuilder> builderPair
                    : PIXMLReader.builderMap.entrySet() )
            {
                Integer key = builderPair.getKey();

                if ( PIXMLReader.copyCount.get( key ) > 0 )
                {
                    String header;
                    try
                    {
                        header = PIXMLReader.getForecastInsertHeader( key );
                    }
                    catch ( SQLException e )
                    {
                        LOGGER.error( "While saving leftover forecasts:", e );
                        continue;
                    }
                    CopyExecutor copier =
                            new CopyExecutor( header,
                                              builderPair.getValue().toString(),
                                              delimiter );
                    copier.setOnRun( ProgressMonitor.onThreadStartHandler() );
                    copier.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
                    Database.storeIngestTask( Database.execute( copier ) );
                    PIXMLReader.builderMap.put( key, new StringBuilder() );
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
	 * @throws SQLException Thrown if the XML could not be read or interpreted properly
	 */

    private void parseHeader( XMLStreamReader reader )
            throws XMLStreamException, SQLException,
            InvalidPropertiesFormatException, InvalidInputDataException
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
					this.currentLID = XML.getXMLText(reader);

					if (currentLID.length() > 5)
					{
					    String shortendID = currentLID.substring(0, 5);

					    if (Features.exists( shortendID ))
						{
							this.currentLID = shortendID;
						}
					}
				}
				else if (localName.equalsIgnoreCase("stationName"))
				{
					//	If we are at the tag for the name of the station, save it to the location
					currentStationName = XML.getXMLText(reader);
				}
				else if(this.isForecast && localName.equalsIgnoreCase("ensembleId"))
				{
				    currentTimeSeriesID = null;
					currentEnsembleID = null;
					//	If we are at the tag for the name of the ensemble, save it to the ensemble
					currentEnsembleName = XML.getXMLText(reader);
				}
				else if(this.isForecast && localName.equalsIgnoreCase("qualifierId"))
				{
				    currentTimeSeriesID = null;
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble qualifier, save it to the ensemble
					//current_ensemble.qualifierID = tag_value(reader);
					currentQualifierID = XML.getXMLText(reader);
				}
				else if(this.isForecast && localName.equalsIgnoreCase("ensembleMemberIndex"))
				{
				    currentTimeSeriesID = null;
					currentEnsembleID = null;
					
					//	If we are at the tag for the ensemble member, save it to the ensemble
					currentEnsembleMemberID = XML.getXMLText(reader);
				}
				else if(this.isForecast && localName.equalsIgnoreCase("forecastDate"))
				{
                    this.forecastDate = PIXMLReader.parseDateTime( reader );
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
					missingValue = Double.parseDouble( XML.getXMLText( reader ) );
				}
				else if (localName.equalsIgnoreCase("startDate"))
                {
                    this.startDate = PIXMLReader.parseDateTime( reader );
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
					currentVariableID = Variables.getVariableID(currentVariableName,
																currentMeasurementUnitID);
				}
			}
			reader.next();
		}
	}

	/**
	 * Reads the date and time from an XML reader that stores the date and time in separate attributes
	 * @param reader The XML Reader positioned at a node containing date and time attributes
     * @return a LocalDateTime representing the parsed value
     */
    private static LocalDateTime parseDateTime( XMLStreamReader reader )
            throws InvalidInputDataException
    {
        LocalDate date = null;
        LocalTime time = null;
		String localName;
		
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			localName = reader.getAttributeLocalName(attributeIndex);

			if (localName.equalsIgnoreCase("date"))
			{
                String dateText = reader.getAttributeValue(attributeIndex);
                date = LocalDate.parse( dateText );
			}
			else if (localName.equalsIgnoreCase("time"))
			{
                String timeText = reader.getAttributeValue(attributeIndex);
                time = LocalTime.parse( timeText );
			}
		}

        if ( date == null || time == null )
        {
            throw new InvalidInputDataException(
                    "Could not parse date and time at line "
                    + reader.getLocation().getLineNumber()
                    + " column " + reader.getLocation().getColumnNumber() );
        }

        return LocalDateTime.of( date, time );
	}
	
	/**
	 * @return The ID of the current ensemble
	 * @throws SQLException Thrown if the ensemble ID could not be retrieved from the cache
	 */
	private int getEnsembleID() throws SQLException {
		if (currentEnsembleID == null) {
			currentEnsembleID = Ensembles.getEnsembleID(currentEnsembleName,
														currentEnsembleMemberID,
														currentQualifierID);
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
	 * @return The ID of the position for the variable being parsed
	 * @throws SQLException Thrown if the ID of the position could not be loaded from the cache
	 */
	private int getVariablePositionID() throws SQLException {
		if (currentVariablePositionID == null) {
			currentVariablePositionID = Features.getVariablePositionID(currentLID,
                                                                       currentStationName,
                                                                       getVariableID());
		}
		return currentVariablePositionID;
	}
	
	/**
	 * @return The ID of the ensemble for the forecast that is tied to a specific variable
	 * @throws SQLException Thrown if intermediary values could not be loaded from their own caches or if interaction
	 * with the database failed.
	 */
	private int getTimeSeriesID()
            throws SQLException
    {
		if (currentTimeSeriesID == null) {
			this.getCurrentTimeSeries().setEnsembleID(getEnsembleID());
            this.getCurrentTimeSeries().setMeasurementUnitID(getMeasurementID());
            this.getCurrentTimeSeries().setVariablePositionID(getVariablePositionID());
            currentTimeSeriesID = this.getCurrentTimeSeries().getTimeSeriesID();
		}
		return currentTimeSeriesID;
	}

	private TimeSeries getCurrentTimeSeries()
            throws SQLException
    {
        if (this.currentTimeSeries == null)
        {
            OffsetDateTime forecastFullDateTime
                    = OffsetDateTime.of( this.getForecastDate(),
                                         this.getZoneOffset() );
            this.currentTimeSeries =
                    new TimeSeries( this.projectDetails.getId(),
									this.getSourceID(),
                                    forecastFullDateTime.format( FORMATTER ) );
        }
        return this.currentTimeSeries;
    }
	
	/**
	 * @return The ID of the variable currently being measured
	 * @throws SQLException Thrown if interaction with the database failed.
	 */
	private int getVariableID() throws SQLException {
		if (currentVariableID == null)
		{
			this.currentVariableID = Variables.getVariableID(currentVariableName,
                                                             currentMeasurementUnit);
		}
		return this.currentVariableID;
	}
	
	/**
	 * @return A valid ID for the source of this PIXML file from the database
	 * @throws SQLException Thrown if an ID could not be retrieved from the database
	 */
	private int getSourceID()
            throws SQLException
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
				OffsetDateTime actualStartDate =
						OffsetDateTime.of( this.getStartDate(),
										   this.getZoneOffset() );
                output_time = actualStartDate.format( FORMATTER );
			}
			currentSourceID = DataSources.getSourceID(getFilename(),
													  output_time,
													  null,
													  this.getHash());
		}
		return currentSourceID;
	}
    
    private boolean variableIsApproved (String name) {
	    boolean approved = true;

	    // If there is a specification for what variables to include
	    if (this.getDataSourceConfig() != null &&
                this.getDataSourceConfig().getVariable() != null &&
                this.getDataSourceConfig().getVariable().getValue() != null)
        {
            // Approve if the passed in variable name matches that of the configured variable name
            approved = this.getDataSourceConfig().getVariable().getValue().isEmpty() ||
                    this.getDataSourceConfig().getVariable().getValue().equalsIgnoreCase(name);
        }

        if (!approved)
        {
            LOGGER.debug("The variable '{}' is not approved. The configuration says the variable should be: '{}'",
                         name,
                         this.getDataSourceConfig().getVariable().getValue());
        }

        return approved;
    }
    
    private boolean ensembleIsApproved (final String name, final String ensembleMemberID) {
	    boolean approved = true;

	    // If there is a configuration for approved ensembles...
	    if (this.getDataSourceConfig() != null &&
                this.getDataSourceConfig().getEnsemble() != null &&
                this.getDataSourceConfig().getEnsemble().size() > 0)
        {
            // Determine if there are instructions to ignore specific ensembles
            final boolean exclusions = Collections.exists(this.getDataSourceConfig().getEnsemble(), EnsembleCondition::isExclude);

            // If the configuration has an exclusion clause...
            if (exclusions)
            {
                // Approve if there are no instructions to exclude this ensemble
                approved = !Collections.exists(this.getDataSourceConfig().getEnsemble(),
                                               (EnsembleCondition ensemble) ->
                                                       ensemble.isExclude() &&
                                                       ensemble.getName().equalsIgnoreCase(name) &&
                                                       ensemble.getMemberId().equalsIgnoreCase(ensembleMemberID) );
            }
            else
            {
                // Only approve if the ensemble has explicit inclusion specified
                approved = Collections.exists(this.getDataSourceConfig().getEnsemble(),
                                              (EnsembleCondition ensemble) ->
                                                      ensemble.isExclude() &&
                                                      ensemble.getName().equalsIgnoreCase(name) &&
                                                      ensemble.getMemberId().equalsIgnoreCase(ensembleMemberID)
                );
            }
        }

        if (!approved)
		{
			LOGGER.error("The ensemble with the name {} and ensemblemember id of {} was not approved.", name, ensembleMemberID);
		}

        return approved;
    }
    
    private boolean featureIsApproved (final String lid) {
	    boolean approved = true;

	    boolean hasLocations = Collections.exists(this.getSpecifiedFeatures(), (Feature feature) -> {
            return feature.getLocationId() != null;
        });

	    if (hasLocations)
        {
            approved = Collections.exists(this.getSpecifiedFeatures(), (Feature feature) -> {
                return feature.getLocationId() != null &&
                       feature.getLocationId()
                               .equalsIgnoreCase( lid );
            });
        }

        return approved;
    }
    
    private boolean seriesIsApproved ()
    {
        boolean ensembleApproved = this.ensembleIsApproved(this.currentEnsembleName, this.currentEnsembleMemberID);

        if (!ensembleApproved)
        {
            LOGGER.debug( "The encounted ensemble (ID: {}, Member: {}) is not approved for this ingest.",
                          String.valueOf( this.currentEnsembleName),
                          String.valueOf( this.currentEnsembleMemberID ));
        }

        boolean featureApproved = this.featureIsApproved(this.currentLID);

        if (!featureApproved)
        {
            LOGGER.debug( "The encountered feature ('{}') is not approved for this ingest.",
                          String.valueOf( this.currentLID));
        }

        boolean variableApproved = this.variableIsApproved(currentVariableName);

        if (!variableApproved)
        {
            LOGGER.debug( "The encountered variable ('{}') is not approved for this ingest.",
                          String.valueOf(this.currentVariableName));
        }
        
        return featureApproved && variableApproved && ensembleApproved;
    }

    /**
     * @return The value specifying a value that is missing from the data set
     * originating from the data source configuration. While parsing the data,
     * if this value is encountered, it indicates that the value should be
     * ignored as it represents invalid data. This should be ignored in data
     * sources that define their own missing value.
     */
    protected Double getSpecifiedMissingValue()
    {
        if (missingValue == null && dataSourceConfig != null)
        {
            DataSourceConfig.Source source = this.getSourceConfig();

            if (source != null && Strings.hasValue( source.getMissingValue() ))
            {
                missingValue = Double.parseDouble(source.getMissingValue());
            }
        }

        return missingValue;
    }

    /**
     * Conditions the passed in value and transforms it into a form suitable to
     * save into the database.
     * <p>
     *     If the passed in value is found to be equal to the specified missing
     *     value, it is set to 'null'
     * </p>
     * @param value The original value
     * @return The conditioned value that is safe to save to the database.
     */
    protected String getValueToSave(String value)
    {
        value = value.trim();

        if (Strings.hasValue(value) &&
			!value.equalsIgnoreCase( "null" ) &&
            this.getSpecifiedMissingValue() != null)
        {
            Double val = Double.parseDouble( value );
            if ( this.getSpecifiedMissingValue().equals( val ) || Precision.equals(val, this.getSpecifiedMissingValue(), EPSILON))
            {
                value = "\\N";
            }
        }
        else if (!Strings.hasValue( value ) || value.equalsIgnoreCase( "null" ) || value.equalsIgnoreCase( "nan" ))
        {
            value = "\\N";
        }

        return value;
    }

    private LocalDateTime getStartDate()
	{
		return this.startDate;
	}

    private LocalDateTime getForecastDate()
    {
		return this.forecastDate;
    }

    private ZoneOffset getZoneOffset()
    {
        return this.zoneOffset;
    }

    /**
     * The most recent time zone offset read from the source, null if not found.
     */
    private ZoneOffset zoneOffset = null;

	/**
	 * The date for when the source was created
	 */
	private String creationDate = null;
	
	/**
	 * The time on the date that the source was created
	 */
	private String creationTime = null;

	/**
	 * The date and time of the first value forecasted
	 */
    private LocalDateTime startDate = null;

    /**
     * The date and time that forecasting began
     */
    private LocalDateTime forecastDate = null;
	
	/**
	 * Indicates whether or not the data is for forecasts. Default is True
	 */
	private boolean isForecast = true;
	
	/**
	 * Basic details about the current ensemble for a current forecast
	 */
	private TimeSeries currentTimeSeries = null;
	
	/**
	 * The value which indicates a null or invalid value from the source
	 */
	private Double missingValue = null;
	
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
	private Integer currentEnsembleID;
	
	/**
	 * The ID for the Ensemble for the forecast that is currently being parsed
	 */
	private Integer currentTimeSeriesID = null;
	
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

    /**
     * The hash code for the source file
     */
	private String hash;

	private final ProjectDetails projectDetails;

    private String getHash()
    {
        return this.hash;
    }

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

	private static String getForecastInsertHeader(Integer lead) throws SQLException
    {
	    synchronized (PIXMLReader.headerMap)
        {
            if (!PIXMLReader.headerMap.containsKey(lead))
            {
                PIXMLReader.headerMap.putIfAbsent(lead, PIXMLReader.createForecastValuePartition(lead));
            }
        }
	    return PIXMLReader.headerMap.get(lead);
    }

    public void setDataSourceConfig(DataSourceConfig dataSourceConfig)
	{
		this.dataSourceConfig = dataSourceConfig;
	}

	private DataSourceConfig getDataSourceConfig()
	{
		return this.dataSourceConfig;
	}

    public void setSourceConfig( DataSourceConfig.Source sourceConfig )
    {
        this.sourceConfig = sourceConfig;
    }

    private DataSourceConfig.Source getSourceConfig()
    {
        return this.sourceConfig;
    }

	public void setSpecifiedFeatures(List<Feature> specifiedFeatures)
    {
        this.specifiedFeatures = specifiedFeatures;
    }

    private List<Feature> getSpecifiedFeatures()
    {
        return this.specifiedFeatures;
    }

    private DataSourceConfig dataSourceConfig;
    private List<Feature> specifiedFeatures;

    private static final Map<Integer, StringBuilder> builderMap = new TreeMap<>();
    private static final Map<Integer, String> headerMap = new TreeMap<>();
    private static final Map<Integer, Integer> copyCount = new TreeMap<>();
    private static final Object groupLock = new Object();
}

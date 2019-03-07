package wres.io.reading.fews;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
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
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Objects;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.datamodel.metadata.TimeScale;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeSeries;
import wres.io.reading.IngestException;
import wres.io.reading.IngestedValues;
import wres.io.reading.InvalidInputDataException;
import wres.system.xml.XMLHelper;
import wres.system.xml.XMLReader;
import wres.util.Strings;

/**
 * @author Christopher Tubbs
 * Loads a PIXML file, iterates through it, and saves all data to the database, whether it is
 * forecast or observation data
 */
public final class PIXMLReader extends XMLReader
{
	private static final Logger LOGGER = LoggerFactory.getLogger(PIXMLReader.class);

    /**
     * Epsilon value used to test floating point equivalency
     */
    private static final double EPSILON = 0.0000001;

    private static final DateTimeFormatter FORMATTER
            = DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss", Locale.US )
                               .withZone( ZoneId.of( "UTC" ) );

    private DataSourceConfig.Source sourceConfig;

    // Start by assuming we must ingest
    private boolean inChargeOfIngest = true;

	/**
	 * Constructor for a reader that may be for forecasts or observations
	 * @param filename The path to the file to read
	 * @param hash the hash code for the source
     * @throws IOException when an attempt to get the file from classpath fails.
	 */
    PIXMLReader( URI filename,
				 String hash )
            throws IOException
	{
		super(filename);
		this.hash = hash;
	}

    public PIXMLReader( URI filename,
                        InputStream inputStream,
                        String hash )
            throws IOException
	{
		super(filename, inputStream);
		this.hash = hash;
	}

	@Override
	protected void parseElement( XMLStreamReader reader )
			throws IOException
	{
        // Must determine if in charge of ingest
        // TODO: actually evaluate inChargeOfIngest before checking it
        // Why is inChargeOfIngest even involved? If this isn't in charge of the
        // ingest, there's been a problem upstream
        if ( !inChargeOfIngest )
        {
            LOGGER.debug( "This PIXMLReader yields for source {}", hash );
            // How to close without going through all elements?
            return;
        }

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
    protected Logger getLogger()
    {
        return LOGGER;
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
            throws XMLStreamException, SQLException, IOException
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
				}
				else if(localName.equalsIgnoreCase("event"))
				{
					parseEvent(reader);
				}
			}
		}
	}


	/**
	 * Removes information about a measurement from an "event" tag. If a sufficient number of events have been
	 * parsed, they are sent to the database to be saved.
	 * @param reader The reader containing the current event tag
     * @throws SQLException when unable to get a series id or unable to save
     * @throws ProjectConfigException when a forecast is missing a forecast date
	 */
	private void parseEvent(XMLStreamReader reader)
            throws SQLException, IngestException
    {
		String value = null;
		String localName;
        LocalDate localDate = null;
        LocalTime localTime = null;
        String dateText = "";
        String timeText = "";

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

		if (!Strings.hasValue( value ))
        {
            LOGGER.debug( "The event at {} {} in '{}' didn't have a value to save.",
                          String.valueOf(dateText),
                          String.valueOf( timeText ),
                          this.getFilename());
            return;
        }
        else if (localDate == null || localTime == null)
        {
            throw new IngestException("An event for " + this.currentLID + " at "
                                      + this.creationDate + " " + this.startDate
                                      + " in " + this.getFilename() + " didn't have "
                                      + "information about when the value was valid. "
                                      + "The source is not properly formed and parsing "
                                      + "cannot continue.");
        }

        LocalDateTime dateTime = LocalDateTime.of( localDate, localTime );
        if (this.getIsForecast())
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
            int leadTimeInHours = (int) leadTime.toMinutes();

            Integer timeseriesID = this.getTimeSeriesID();

            if ( this.inChargeOfIngest  )
            {
                IngestedValues.addTimeSeriesValue(
                        timeseriesID,
                        leadTimeInHours,
                        this.getValueToSave( value )
                );
            }
            else
            {
                LOGGER.trace( "This PIXMLReader yields for source {}", hash );
            }
        }
        else
        {
            if (value.trim().equalsIgnoreCase( "nan" ))
            {
                LOGGER.trace( "NaN encountered." );
            }

            OffsetDateTime offsetDateTime = OffsetDateTime.of( dateTime, this.getZoneOffset() )
                                                          .withOffsetSameInstant( ZoneOffset.UTC );

            this.addObservedEvent( offsetDateTime, this.getValueToSave( value ) );
        }
	}
	
	/**
	 * Adds measurement information to the current insert script in the form of observation data
	 * @param observedTime The time when the measurement was taken
	 * @param observedValue The value retrieved from the XML
	 * @throws SQLException Any possible error encountered while trying to retrieve the variable position id or the id of the measurement uni
	 */
	private void addObservedEvent(OffsetDateTime observedTime, Double observedValue)
			throws SQLException
	{
	    IngestedValues.observed( observedValue )
                      .at(observedTime)
                      .forVariableAndFeatureID( this.getVariableFeatureID() )
                      .measuredIn( this.getMeasurementID() )
                      .inSource( this.getSourceID() )
                      .scaleOf( this.scalePeriod )
                      .scaledBy( this.scaleFunction )
                      .add();
	}


	/**
	 * Interprets the information within PIXML "header" tags
	 * @param reader The XML reader, positioned at a "header" tag
	 * @throws SQLException Thrown if the XML could not be read or interpreted properly
	 */

    private void parseHeader( XMLStreamReader reader )
			throws XMLStreamException,
			SQLException,
			IngestException
    {
		//	If the current tag is the header tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("header"))
		{
			reader.next();
		}
		String localName;
		creationDate = null;
		creationTime = null;
		this.scalePeriod = null;
		this.scaleFunction = TimeScale.TimeScaleFunction.UNKNOWN;
		this.timeStep = null;

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

				if ( this.getIsForecast() )
				{
					parseForecastHeaderElements( reader, localName );
				}

				if (localName.equalsIgnoreCase("locationId"))
				{
				    // TODO: Set the LID on a FeatureDetails object; don't just store the LID
					//	If we are at the tag for the location id, save it to the location metadata
					this.currentLID = XMLHelper.getXMLText( reader);
					this.currentVariableFeatureID = null;
					this.currentTimeSeriesID = null;
					this.currentTimeSeries = null;

					if (currentLID.length() > 5)
					{
					    String shortendID = currentLID.substring(0, 5);

					    if (Features.lidExists( shortendID ))
						{
							this.currentLID = shortendID;
						}
					}
				}
				else if(localName.equalsIgnoreCase("units"))
				{
					currentMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(XMLHelper.getXMLText(reader));
				}
				else if(localName.equalsIgnoreCase("missVal"))
				{
					// If we are at the tag for the missing value definition, record it
					missingValue = Double.parseDouble( XMLHelper.getXMLText( reader ) );
				}
				else if (localName.equalsIgnoreCase("startDate"))
                {
                    this.startDate = PIXMLReader.parseDateTime( reader );
				}
				else if (XMLHelper.tagIs(reader, "creationDate")) {
					creationDate = XMLHelper.getXMLText(reader);
				}
				else if (XMLHelper.tagIs(reader, "creationTime")) {
					creationTime = XMLHelper.getXMLText(reader);
				}
				else if (localName.equalsIgnoreCase("parameterId"))
				{
					currentVariableName = XMLHelper.getXMLText(reader);
					currentVariableID = Variables.getVariableID(currentVariableName);
				}
				else if ( localName.equalsIgnoreCase("forecastDate") )
				{
					if (!this.isForecast)
					{
						throw new IngestException("The file '" + this.getFilename() +
											  "' cannot be ingested as an "
											  + "observation since it is a forecast.");
					}
					this.forecastDate = PIXMLReader.parseDateTime( reader );
				}
				else if ( localName.equalsIgnoreCase( "type" ))
                {
                    // See #59438
				    if (XMLHelper.getXMLText( reader ).equalsIgnoreCase( "instantaneous" ))
                    {
                        this.scalePeriod = Duration.ofMinutes( 1 );
                    }
                }
                else if ( localName.equalsIgnoreCase( "timeStep" ))
                {
                    String unit = XMLHelper.getAttributeValue( reader, "unit" ) + "s";
                    unit = unit.toUpperCase();

                    Integer amount = Integer.parseInt( XMLHelper.getAttributeValue( reader, "multiplier" ) );

                    this.timeStep = Duration.of( amount, ChronoUnit.valueOf( unit ) );
                }

			}
			reader.next();
		}
		
		// See #59438
		// For accumulative data, the scalePeriod has not been set, and this is equal
		// to the timeStep
		if( Objects.isNull( this.scalePeriod ) )
		{
		    this.scalePeriod = this.timeStep;
		}
	}


    /**
     * Helper to read forecast-specific tags out of the header.
     * @param reader an xml reader positioned inside the header of PI-XML
     * @param localName the name of the tag the reader is currently on
     */

    private void parseForecastHeaderElements( XMLStreamReader reader,
                                              String localName )
            throws XMLStreamException, InvalidInputDataException
    {
        if ( localName.equalsIgnoreCase("ensembleId") ||
			 localName.equalsIgnoreCase( "ensembleMemberId" ) )
        {
            currentTimeSeriesID = null;
            currentEnsembleID = null;
            //    If we are at the tag for the name of the ensemble, save it to the ensemble
            currentEnsembleName = XMLHelper.getXMLText(reader);
        }
        else if ( localName.equalsIgnoreCase("qualifierId") )
        {
            currentTimeSeriesID = null;
            currentEnsembleID = null;

            //    If we are at the tag for the ensemble qualifier, save it to the ensemble
            //current_ensemble.qualifierID = tag_value(reader);
            currentQualifierID = XMLHelper.getXMLText(reader);
        }
        else if ( localName.equalsIgnoreCase("ensembleMemberIndex") )
        {
            currentTimeSeriesID = null;
            currentEnsembleID = null;

            //    If we are at the tag for the ensemble member, save it to the ensemble
            String member = XMLHelper.getXMLText( reader );

            if (Strings.hasValue( member ))
            {
                currentEnsembleMemberID = Integer.parseInt( member );
            }
        }
        else if ( localName.equalsIgnoreCase("forecastDate") )
        {
            this.forecastDate = PIXMLReader.parseDateTime( reader );
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
	private int getVariableFeatureID() throws SQLException
	{
		if (currentVariableFeatureID == null)
		{
		    // TODO: This needs to rely on a FeatureDetails object, not an LID
            //       If we store additional information, new values become easier
            //       to use
			currentVariableFeatureID = Features.getVariableFeatureIDByLID( currentLID,
                                                                             getVariableID());
		}
		return currentVariableFeatureID;
	}
	
	/**
	 * @return The ID of the ensemble for the forecast that is tied to a specific variable
	 * @throws SQLException Thrown if intermediary values could not be loaded from their own caches or if interaction
	 * with the database failed.
	 */
	private int getTimeSeriesID()
			throws SQLException
	{
		if (currentTimeSeriesID == null)
		{
			this.getCurrentTimeSeries().setEnsembleID(getEnsembleID());
            this.getCurrentTimeSeries().setMeasurementUnitID(getMeasurementID());
            this.getCurrentTimeSeries().setVariableFeatureID(getVariableFeatureID());
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
                    new TimeSeries( this.getSourceID(),
                                    forecastFullDateTime.format( FORMATTER ) );

            // Set the time scale information
            if( this.scalePeriod != null && this.scaleFunction != null )
            {
                this.currentTimeSeries.setTimeScale( TimeScale.of( this.scalePeriod, this.scaleFunction ) );
            }

            LOGGER.trace( "Created time series {} in reader of {} because currentTimeSeries was null.",
                          this.currentTimeSeries,
                          this.hash);
        }
        return this.currentTimeSeries;
    }
	
	/**
	 * @return The ID of the variable currently being measured
	 * @throws SQLException Thrown if interaction with the database failed.
	 */
	private int getVariableID() throws SQLException
    {
		if (currentVariableID == null)
		{
			this.currentVariableID = Variables.getVariableID(currentVariableName);
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

			// TODO: Modify the cache to perform this work
            // In order to interrogate the Cache, we need the key, not the
            // actual SourceDetails class itself.

            SourceDetails.SourceKey sourceKey =
                    new SourceDetails.SourceKey( this.getFilename(),
                                                 output_time,
                                                 null,
                                                 this.getHash() );

            // Ask the cache "do you have this source?"
            boolean wasInCache = DataSources.isCached( sourceKey );
            boolean wasThisReaderTheOneThatInserted = false;
            SourceDetails sourceDetails;

            if ( !wasInCache )
            {
                // We *might* be the one in charge of doing this source ingest.
                sourceDetails = new SourceDetails( sourceKey );
                sourceDetails.save();
                if ( sourceDetails.performedInsert() )
                {
                    // Now we have the definitive answer from the database.
                    wasThisReaderTheOneThatInserted = true;

                    // Now that ball is in our court we should put in cache
                    DataSources.put( sourceDetails );
                    // // Older, implicit way:
                    // DataSources.hasSource( this.getHash() );
                }
            }

            // Regardless of whether we were the ones or not, get it from cache
            currentSourceID = DataSources.getActiveSourceID( this.getHash() );

			// Mark whether this reader is the one to perform ingest or yield.
            inChargeOfIngest = wasThisReaderTheOneThatInserted;
		}
		return currentSourceID;
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

    private boolean getIsForecast()
	{
		if (this.isForecast == null)
		{
			this.isForecast = ConfigHelper.isForecast( this.getDataSourceConfig() );
		}
		return this.isForecast;
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
    private Double getValueToSave(String value)
    {
        if (!Strings.hasValue( value ))
        {
            return null;
        }

        value = value.trim();
        Double val = null;

        if (Strings.hasValue(value) &&
			!value.equalsIgnoreCase( "null" ) &&
            this.getSpecifiedMissingValue() != null)
        {
            val = Double.parseDouble( value );
            if ( val.equals( this.getSpecifiedMissingValue() ) || Precision.equals(val, this.getSpecifiedMissingValue(), EPSILON))
            {
                val = null;
            }
        }

        return val;
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
	private Boolean isForecast;
	
	/**
	 * Basic details about the current ensemble for a current forecast
	 */
	private TimeSeries currentTimeSeries = null;
	
	/**
	 * The value which indicates a null or invalid value from the source
	 */
	private Double missingValue = null;
	
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
	private Integer currentVariableFeatureID = null;

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
	 * The name of the ensemble currently being parsed
	 */
	private String currentEnsembleName = null;
	
	/**
	 * The member ID of the current ensemble
	 */
	private Integer currentEnsembleMemberID = null;
	
	/**
	 * The name of the variable whose values are currently being parsed 
	 */
	private String currentVariableName = null;

	private Duration timeStep;
	private Duration scalePeriod;
	private TimeScale.TimeScaleFunction scaleFunction;

    /**
     * The hash code for the source file
     */
	private final String hash;

    private String getHash()
    {
        return this.hash;
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

    private DataSourceConfig dataSourceConfig;
}

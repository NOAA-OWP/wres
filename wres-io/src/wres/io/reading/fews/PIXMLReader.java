package wres.io.reading.fews;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.datamodel.time.ReferenceTimeType.UNKNOWN;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.TimeScales;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.InvalidInputDataException;
import wres.io.reading.ReaderUtilities;
import wres.io.utilities.Database;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.system.xml.XMLHelper;
import wres.system.xml.XMLReader;
import wres.util.Strings;

/**
 * Loads a PIXML file, iterates through it, and saves all data to the database, whether it is
 * forecast or observation data
 * @author Christopher Tubbs
 * @author James Brown
 */
public final class PIXMLReader extends XMLReader
{
	private static final Logger LOGGER = LoggerFactory.getLogger(PIXMLReader.class);

    /** A placeholder reference datetime for timeseries without one. */
    private static final Instant PLACEHOLDER_REFERENCE_DATETIME = Instant.MIN;
    private static final String DEFAULT_ENSEMBLE_NAME = "default";

    /**
     * http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd
     *
     * See "missVal" documentation: "Defaults to NaN if left empty"
     */
    private static final double PIXML_DEFAULT_MISSING_VALUE = Double.NaN;

    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final TimeScales timeScalesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;

    private final List<IngestResult> ingested;

    private TimeSeriesMetadata currentTimeSeriesMetadata = null;
    private String currentTraceName = null;
    private SortedMap<String, SortedMap<Instant,Double>> traceValues = new TreeMap<>();
    private int highestLineNumber = 0;


    /**
     * Constructor for a reader that may be for forecasts or observations
     * @throws IOException when an attempt to get the file from classpath fails.
     */
    PIXMLReader( SystemSettings systemSettings,
                 Database database,
                 Features featuresCache,
                 TimeScales timeScalesCache,
                 Ensembles ensemblesCache,
                 MeasurementUnits measurementUnitsCache,
                 ProjectConfig projectConfig,
                 DataSource dataSource,
                 DatabaseLockManager lockManager )
            throws IOException
    {
        super( dataSource.getUri(),
               dataSource.getDisposition() == DataDisposition.XML_FI_TIMESERIES );
        this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.timeScalesCache = timeScalesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.ingested = new ArrayList<>();
	}

    public PIXMLReader( SystemSettings systemSettings,
                        Database database,
                        Features featuresCache,
                        TimeScales timeScalesCache,
                        Ensembles ensemblesCache,
                        MeasurementUnits measurementUnitsCache,
                        ProjectConfig projectConfig,
                        DataSource dataSource,
                        InputStream inputStream,
                        DatabaseLockManager lockManager )
            throws IOException
	{
        super( dataSource.getUri(),
               inputStream,
               dataSource.getDisposition() == DataDisposition.XML_FI_TIMESERIES );
		this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.timeScalesCache = timeScalesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.ingested = new ArrayList<>();
	}

	private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private TimeScales getTimeScalesCache()
    {
        return this.timeScalesCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    private DataSource getDataSource()
    {
        return this.dataSource;
    }

    @Override
    protected void parseElement( XMLStreamReader reader )
            throws IOException
    {
        if ( reader.isStartElement() )
        {
            String localName = reader.getLocalName();

            if ( localName.equalsIgnoreCase( "timeSeries" ) )
            {
                LOGGER.debug( "Read first element 'timeSeries' of {}",
                              this.getFilename() );

                if ( !this.traceValues.isEmpty() )
                {
                    LOGGER.debug( "Ingesting due to non-empty tracevalues" );

                    this.createAndIngestTimeSeries( this.currentTimeSeriesMetadata,
                                                    this.traceValues,
                                                    this.currentTraceName,
                                                    reader.getLocation()
                                                          .getLineNumber() );

                    // Reset the temporary data structures for timeseries
                    this.currentTimeSeriesMetadata = null;
                    this.traceValues = new TreeMap<>();
                    this.currentTraceName = null;
                }

            }
            else if ( localName.equalsIgnoreCase( "timeZone" ) )
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

        this.highestLineNumber = reader.getLocation()
                                       .getLineNumber();
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
            throws XMLStreamException, SQLException
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
                LOGGER.warn( "The zone offset specified {}{}{}{}{}{}{}{}",
                             "for this source (",
                             configuredOffset,
                             ") did not match what was in the source data (",
                             this.getZoneOffset(),
                             "). It is best to NOT specify the zone for PI-XML",
                             " sources in the project declaration because WRES ",
                             "ignores it and uses the zone offset found in-",
                             "band in the data");
            }
        }
        
		String localName;

        //	Loop through every element in a series (header -> entry -> entry -> ... )
        while ( reader.hasNext() )
        {
            reader.next();

            if ( reader.isEndElement() && reader.getLocalName().equalsIgnoreCase( "series" ) )
            {
                break;
            }
            else if ( reader.isStartElement() )
            {
                localName = reader.getLocalName();

                if ( localName.equalsIgnoreCase( "header" ) )
                {
                    this.parseHeader( reader );
                }
                else if ( localName.equalsIgnoreCase( "event" ) )
                {
                    this.parseEvent( reader );
                }
            }
        }
	}


	/**
	 * Removes information about a measurement from an "event" tag. If a sufficient number of events have been
	 * parsed, they are sent to the database to be saved.
	 * @param reader The reader containing the current event tag
     * @throws ProjectConfigException when a forecast is missing a forecast date
     * @throws PreIngestException When data is improperly formatted.
	 */

	private void parseEvent(XMLStreamReader reader)
    {
        // #102285
        if ( Objects.isNull( this.currentTraceName ) )
        {
            throw new PreIngestException( "An event for metadata '"
                                          + this.currentTimeSeriesMetadata
                                          + "' in "
                                          + this.getFilename()
                                          + " did not have a trace name for the current trace. "
                                          + "Either the source is not properly formed or the "
                                          + "header was not read properly. Parsing cannot "
                                          + "continue." );
        }

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

		LOGGER.debug( "Parsed an event: date={}, time={}, value={}.", dateText, timeText, value );
		
		if ( Objects.isNull( value ) || value.isBlank() )
        {
            LOGGER.debug( "The event at {} {} in '{}' didn't have a value to save.",
                          dateText, timeText, this.getFilename() );
            return;
        }
        else if (localDate == null || localTime == null)
        {
            throw new PreIngestException( "An event for " + this.currentTimeSeriesMetadata
                                          + " in " + this.getFilename() + " didn't have "
                                          + "information about when the value was valid. "
                                          + "The source is not properly formed and parsing "
                                          + "cannot continue.");
        }

        LocalDateTime dateTime = LocalDateTime.of( localDate, localTime );
        Instant fullDateTime = OffsetDateTime.of( dateTime, this.zoneOffset )
                                             .toInstant();
        
        SortedMap<Instant,Double> values = this.traceValues.get( this.currentTraceName );

        if ( Objects.isNull( values ) )
        {
            LOGGER.debug( "Creating new values because trace '{}' not found.",
                          this.currentTraceName );
            values = new TreeMap<>();
            this.traceValues.put( this.currentTraceName, values );
        }

        double numericValue = this.getValueToSave( value );
        LOGGER.trace( "About to save event at {} with value {} into values {}",
                      fullDateTime, numericValue, values );
        values.put( fullDateTime, numericValue );
	}



	/**
	 * Interprets the information within PIXML "header" tags
	 * @param reader The XML reader, positioned at a "header" tag
	 */

    private void parseHeader( XMLStreamReader reader )
			throws XMLStreamException,
			IngestException
    {
		//	If the current tag is the header tag itself, move on to the next tag
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("header"))
		{
			reader.next();
		}

        String localName;
		Duration scalePeriod = null;
		TimeScaleFunction scaleFunction = TimeScaleFunction.UNKNOWN;
		Duration timeStep = null;
        LocalDateTime forecastDate = null;
        String locationName = null;
        String variableName = null;
        String unitName = null;
        String traceName = DEFAULT_ENSEMBLE_NAME;
        String ensembleMemberId = null;
        String ensembleMemberIndex = null;
        double missingValue = PIXML_DEFAULT_MISSING_VALUE;
        String locationLongName = null;
        String locationStationName = null;
        String locationDescription = null;
        Double latitude = null;
        Double longitude = null;
        Double x = null;
        Double y = null;
        Double z = null;

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
				    // Change for 5.0: just store location verbatim. No magic.
					locationName = XMLHelper.getXMLText( reader);
				}
				else if(localName.equalsIgnoreCase("units"))
				{
					unitName = XMLHelper.getXMLText( reader );
				}
				else if(localName.equalsIgnoreCase("missVal"))
				{
					// If we are at the tag for the missing value definition, record it
					missingValue = Double.parseDouble( XMLHelper.getXMLText( reader ) );
				}
				else if (localName.equalsIgnoreCase("parameterId"))
				{
					variableName = XMLHelper.getXMLText( reader );
				}
				else if ( localName.equalsIgnoreCase("forecastDate") )
				{
                    forecastDate = PIXMLReader.parseDateTime( reader );
                }
				else if ( localName.equalsIgnoreCase( "type" ))
                {
                    // See #59438
				    if (XMLHelper.getXMLText( reader ).equalsIgnoreCase( "instantaneous" ))
                    {
                        scalePeriod = Duration.ofMillis( 1 );
                    }
                }
                else if ( localName.equalsIgnoreCase( "timeStep" ))
                {
                    String unit = XMLHelper.getAttributeValue( reader, "unit" ) + "s";
                    unit = unit.toUpperCase();

                    int amount = Integer.parseInt( XMLHelper.getAttributeValue( reader, "multiplier" ) );
                    timeStep = Duration.of( amount, ChronoUnit.valueOf( unit ) );
                }
                else if ( localName.equalsIgnoreCase( "ensembleMemberId" ) )
                {
                    ensembleMemberId = XMLHelper.getXMLText( reader );
                }
                else if ( localName.equalsIgnoreCase("ensembleMemberIndex") )
                {
                    ensembleMemberIndex = XMLHelper.getXMLText( reader );
                }
                else if ( localName.equalsIgnoreCase( "longName" ) )
                {
                    locationLongName = XMLHelper.getXMLText( reader );
                }
                else if ( localName.equalsIgnoreCase( "stationName" ) )
                {
                    locationStationName = XMLHelper.getXMLText( reader );
                }
                else if ( localName.equalsIgnoreCase( "lat" ) )
                {
                    String rawLatitude = XMLHelper.getXMLText( reader );
                    latitude = Double.parseDouble( rawLatitude );
                }
                else if ( localName.equalsIgnoreCase( "lon" ) )
                {
                    String rawLongitude = XMLHelper.getXMLText( reader );
                    longitude = Double.parseDouble( rawLongitude );
                }

                else if ( localName.equalsIgnoreCase( "x" ) )
                {
                    String rawX = XMLHelper.getXMLText( reader );
                    x = Double.parseDouble( rawX );
                }
                else if ( localName.equalsIgnoreCase( "y" ) )
                {
                    String rawY = XMLHelper.getXMLText( reader );
                    y = Double.parseDouble( rawY );
                }
                else if ( localName.equalsIgnoreCase( "z" ) )
                {
                    String rawZ = XMLHelper.getXMLText( reader );
                    z = Double.parseDouble( rawZ );
                }
			}

			reader.next();
		}

        if ( Objects.nonNull( ensembleMemberId )
             && Objects.nonNull( ensembleMemberIndex ) )
        {
            throw new PreIngestException(
                    "Invalid data in PI-XML source '"  + this.getFilename()
                    + "' near line " + this.highestLineNumber
                    + ": a trace may have either an ensembleMemberId OR an "
                    + "ensembleMemberIndex but not both. Found ensembleMemberId"
                    + " '" + ensembleMemberId + "' and ensembleMemberIndex of '"
                    + ensembleMemberIndex + "'. For more details see "
                    + "http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" );
        }

        if ( Objects.nonNull( ensembleMemberId ) )
        {
            traceName = ensembleMemberId;
        }
        else if ( Objects.nonNull( ensembleMemberIndex ) )
        {
            traceName = ensembleMemberIndex;
        }

        if ( locationStationName != null )
        {
            locationDescription = locationStationName;
        }

        if ( locationLongName != null )
        {
            // Append the long name to description when already set with station
            if ( locationDescription != null )
            {
                locationDescription += " " + locationLongName;
            }
            else
            {
                locationDescription = locationLongName;
            }
        }

		// See #59438
		// For accumulative data, the scalePeriod has not been set, and this is equal
		// to the timeStep
		if( Objects.isNull( scalePeriod ) )
		{
		    scalePeriod = timeStep;
		}

		TimeScaleOuter scale = TimeScaleOuter.of( scalePeriod, scaleFunction );

        Map<ReferenceTimeType,Instant> basisDatetimes = new HashMap<>( 1 );

        if ( Objects.nonNull( forecastDate ) )
        {
            Instant t0 = OffsetDateTime.of( forecastDate, this.getZoneOffset() )
                                       .toInstant();
            basisDatetimes.put( ReferenceTimeType.T0, t0 );
        }
        else
        {
            basisDatetimes.put( UNKNOWN, PLACEHOLDER_REFERENCE_DATETIME );
        }

        String locationWkt = null;

        // When x and y are present, prefer those to lon, lat.
        // Going to Double back to String seems frivolous but it validates data.
        if ( Objects.nonNull( x ) && Objects.nonNull( y ) )
        {
            StringJoiner wktGeometry = new StringJoiner( " " );
            wktGeometry.add( "POINT (");
            wktGeometry.add( x.toString() );
            wktGeometry.add( y.toString() );

            if ( Objects.nonNull( z ) )
            {
                wktGeometry.add( z.toString() );
            }
            wktGeometry.add( ")" );
            locationWkt = wktGeometry.toString();
        }
        else if ( Objects.nonNull( latitude ) && Objects.nonNull( longitude ) )
        {
            StringJoiner wktGeometry = new StringJoiner( " " );
            wktGeometry.add( "POINT (" );
            wktGeometry.add( longitude.toString() );
            wktGeometry.add( latitude.toString() );
            wktGeometry.add( ")" );
            locationWkt = wktGeometry.toString();
        }

        LOGGER.debug( "Parsed PI-XML header: scalePeriod={}, scaleFunction={}, timeStep={}, forecastDate={}, "
                      + "locationName={}, variableName={}, unitName={}, traceName={}, ensembleMemberId={}, "
                      + "ensembleMemberIndex={}, missingValue={}, locationLongName={}, locationStationName={}, "
                      + "locationDescription={}, latitude={}, longitude={}, x={}, y={}, z={}.",
                      scalePeriod,
                      scaleFunction,
                      timeStep,
                      forecastDate,
                      locationName,
                      variableName,
                      unitName,
                      traceName,
                      ensembleMemberId,
                      ensembleMemberIndex,
                      missingValue,
                      locationLongName,
                      locationStationName,
                      locationDescription,
                      latitude,
                      longitude,
                      x,
                      y,
                      z );
        
        
        Geometry geometry = MessageFactory.getGeometry( locationName, 
                                                        locationDescription,
                                                        null,
                                                        locationWkt );
        FeatureKey feature = FeatureKey.of( geometry );

        TimeSeriesMetadata justParsed = TimeSeriesMetadata.of( basisDatetimes,
                                                               scale,
                                                               variableName,
                                                               feature,
                                                               unitName );

        // If we encounter a new header, that means a previous timeseries trace
        // was actually a full trace and needs ingest.
        if ( !justParsed.equals( this.currentTimeSeriesMetadata )
             && !this.traceValues.isEmpty() )
        {
            LOGGER.debug( "Saving a trace as a standalone timeseries because {} not equal to {}",
                          justParsed, this.currentTimeSeriesMetadata );
            
            this.createAndIngestTimeSeries( this.currentTimeSeriesMetadata,
                                            this.traceValues,
                                            this.currentTraceName,
                                            this.highestLineNumber );
            
            this.traceValues = new TreeMap<>();
        }

        this.missingValue = missingValue;
        this.currentTraceName = traceName;

        // Create new trace for each header in PI-XML data.
        this.currentTimeSeriesMetadata = justParsed;
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
     * @return The value specifying a value that is missing from the data set
     * originating from the data source configuration.
     */
    protected double getSpecifiedMissingValue()
    {
        if ( missingValue == PIXML_DEFAULT_MISSING_VALUE
             && this.getDataSourceConfig() != null)
        {
            DataSourceConfig.Source source = this.getSourceConfig();

            if ( Objects.nonNull( source )
                 && Objects.nonNull( source.getMissingValue() )
                 && !source.getMissingValue().isBlank() )
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
     *     value, it is set to WRES' Missing Value
     * </p>
     * @param value The original value
     * @return The conditioned value that is safe to save to the database.
     */
    private double getValueToSave( String value )
    {
        if ( Objects.isNull( value ) || value.isBlank() )
        {
            return MissingValues.DOUBLE;
        }

        value = value.strip();
        double val = MissingValues.DOUBLE;

        if (Strings.hasValue(value) &&
            !value.equalsIgnoreCase( "null" ) )
        {
            val = Double.parseDouble( value );

            if ( val == this.getSpecifiedMissingValue()
                 || Precision.equals( val, this.getSpecifiedMissingValue() ) )
            {
                return MissingValues.DOUBLE;
            }
        }

        return val;
    }


    /**
     * After parsing, do remainder of ingest.
     * @throws IngestException When finishing ingest fails
     */

    @Override
    protected void completeParsing() throws IngestException
    {
        if ( !this.traceValues.isEmpty() )
        {
            LOGGER.debug( "Finished parsing, saving timeseries" );

            this.createAndIngestTimeSeries( this.currentTimeSeriesMetadata,
                                            this.traceValues,
                                            this.currentTraceName,
                                            this.highestLineNumber );
        }

        this.completeIngest();
    }

    /**
     * Build a timeseries out of temporary data structures and then ingests it.
     *
     * When there is a placeholder reference datetime, replace it with the
     * latest valid datetime found as "latest observation." This means there was
     * no reference datetime found in the CSV, but until the WRES db schema is
     * ready to store any kind of timeseries with 0, 1, or N reference datetimes
     * we are required to specify something here.
     *
     * @param timeSeriesMetadata The metadata for most-recently-parsed data.
     * @param ensembleValues The most-recently-parsed data in sorted map form.
     * @param lastEnsembleName The most-recently-parsed ensemble name.
     * @param lineNumber The most-recently-parsed line number in the csv source.
     * @return A TimeSeries either of Double or Ensemble, ready for ingest.
     * @throws PreIngestException When something goes wrong.
     */
    private void createAndIngestTimeSeries( TimeSeriesMetadata timeSeriesMetadata,
                                            SortedMap<String, SortedMap<Instant, Double>> ensembleValues,
                                            String lastEnsembleName,
                                            int lineNumber )
    {
        LOGGER.debug( "Creating a time-series with {}, {}, {}, {}",
                      timeSeriesMetadata,
                      ensembleValues,
                      lastEnsembleName,
                      lineNumber );

        TimeSeriesMetadata metadata;
        Collection<Instant> referenceDatetimes =
                timeSeriesMetadata.getReferenceTimes()
                                  .values();

        // When there are no reference datetimes, use latest value
        // by valid datetime. (Eventually we should remove the
        // restriction of requiring a reference datetime when db is
        // ready for it to be relaxed)
        if ( referenceDatetimes.size() == 1
             && referenceDatetimes.contains( PLACEHOLDER_REFERENCE_DATETIME ) )
        {
            LOGGER.debug( "Found placeholder reference datetime in {}",
                          timeSeriesMetadata );
            Instant latestDatetime = ensembleValues.get( lastEnsembleName )
                                                   .lastKey();
            metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.LATEST_OBSERVATION, latestDatetime ),
                                              timeSeriesMetadata.getTimeScale(),
                                              timeSeriesMetadata.getVariableName(),
                                              timeSeriesMetadata.getFeature(),
                                              timeSeriesMetadata.getUnit() );
        }
        else
        {
            LOGGER.debug( "Found NO placeholder reference datetime in {}",
                          timeSeriesMetadata );
            metadata = timeSeriesMetadata;
        }

        // Check if this is actually an ensemble or single trace
        if ( ensembleValues.size() == 1
             && ensembleValues.firstKey()
                              .equals( DEFAULT_ENSEMBLE_NAME ) )
        {
            TimeSeries<Double> timeSeries = ReaderUtilities.transform( metadata,
                                                                       ensembleValues.get( DEFAULT_ENSEMBLE_NAME ),
                                                                       lineNumber );

            this.ingestSingleValuedTimeSeries( timeSeries );
        }
        else
        {
            TimeSeries<Ensemble> timeSeries = ReaderUtilities.transformEnsemble( metadata,
                                                                                 ensembleValues,
                                                                                 lineNumber,
                                                                                 this.getDataSource()
                                                                                     .getUri() );

            this.ingestEnsembleTimeSeries( timeSeries );
        }
    }

    /**
     * Create an ingester for the given timeseries and ingest in current Thread.
     * @param timeSeries The timeSeries to ingest.
     * @throws IngestException When anything goes wrong related to ingest.
     */

    private void ingestEnsembleTimeSeries( wres.datamodel.time.TimeSeries<Ensemble> timeSeries )
            throws IngestException
    {
        TimeSeriesIngester timeSeriesIngester =
                this.createTimeSeriesIngester( this.getSystemSettings(),
                                               this.getDatabase(),
                                               this.getFeaturesCache(),
                                               this.getTimeScalesCache(),
                                               this.getEnsemblesCache(),
                                               this.getMeasurementUnitsCache(),
                                               this.getProjectConfig(),
                                               this.getDataSource(),
                                               this.getLockManager() );
        try
        {
            List<IngestResult> ingestResults = timeSeriesIngester.ingestEnsembleTimeSeries( timeSeries );
            this.ingested.addAll( ingestResults );
        }
        catch ( IngestException ie )
        {
            throw new IngestException( "Failed to ingest data from "
                                       + this.getFilename() + ":", ie );
        }
    }

    /**
     * Create an ingester for the given timeseries and ingest in current Thread.
     * @param timeSeries The timeSeries to ingest.
     * @throws IngestException When anything goes wrong related to ingest.
     */

    private void ingestSingleValuedTimeSeries( wres.datamodel.time.TimeSeries<Double> timeSeries )
            throws IngestException
    {
        TimeSeriesIngester timeSeriesIngester =
                this.createTimeSeriesIngester( this.getSystemSettings(),
                                               this.getDatabase(),
                                               this.getFeaturesCache(),
                                               this.getTimeScalesCache(),
                                               this.getEnsemblesCache(),
                                               this.getMeasurementUnitsCache(),
                                               this.getProjectConfig(),
                                               this.getDataSource(),
                                               this.getLockManager() );
        try
        {
            List<IngestResult> ingestResults = timeSeriesIngester.ingestSingleValuedTimeSeries( timeSeries );
            this.ingested.addAll( ingestResults );
        }
        catch ( IngestException ie )
        {
            throw new IngestException( "Failed to ingest data from "
                                       + this.getFilename()
                                       + ":",
                                       ie );
        }
    }   

    private void completeIngest()
    {
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Parsed and ingested {} timeseries from {}",
                          ingested.size(),
                          this.getDataSource()
                              .getUri() );
        }
    }



    public List<IngestResult> getIngestResults()
    {
        return Collections.unmodifiableList( this.ingested );
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
	 * The value which indicates a null or invalid value from the source.
     * PI-XML xsd says NaN is missing value if unspecified.
     */
    private double missingValue = PIXML_DEFAULT_MISSING_VALUE;

	private DataSourceConfig getDataSourceConfig()
	{
		return this.dataSource.getContext();
	}


    private DataSourceConfig.Source getSourceConfig()
    {
        return this.dataSource.getSource();
    }



    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @return a TimeSeriesIngester
     */

    TimeSeriesIngester createTimeSeriesIngester( SystemSettings systemSettings,
                                                 Database database,
                                                 Features featuresCache,
                                                 TimeScales timeScalesCache,
                                                 Ensembles ensemblesCache,
                                                 MeasurementUnits measurementUnitsCache,
                                                 ProjectConfig projectConfig,
                                                 DataSource dataSource,
                                                 DatabaseLockManager lockManager )
    {
        return TimeSeriesIngester.of( systemSettings,
                                      database,
                                      featuresCache,
                                      timeScalesCache,
                                      ensemblesCache,
                                      measurementUnitsCache,
                                      projectConfig,
                                      dataSource,
                                      lockManager );
    }
}

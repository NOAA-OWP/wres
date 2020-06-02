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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.datamodel.time.ReferenceTimeType.LATEST_OBSERVATION;
import static wres.datamodel.time.ReferenceTimeType.UNKNOWN;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.InvalidInputDataException;
import wres.io.reading.PreIngestException;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
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

    /** A placeholder reference datetime for timeseries without one. */
    private static final Instant PLACEHOLDER_REFERENCE_DATETIME = Instant.MIN;
    private static final String DEFAULT_ENSEMBLE_NAME = "default";

    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final Variables variablesCache;
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
                 Variables variablesCache,
                 Ensembles ensemblesCache,
                 MeasurementUnits measurementUnitsCache,
                 ProjectConfig projectConfig,
                 DataSource dataSource,
                 DatabaseLockManager lockManager )
            throws IOException
	{
		super( dataSource.getUri() );
		this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
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
                        Variables variablesCache,
                        Ensembles ensemblesCache,
                        MeasurementUnits measurementUnitsCache,
                        ProjectConfig projectConfig,
                        DataSource dataSource,
                        InputStream inputStream,
                        DatabaseLockManager lockManager )
            throws IOException
	{
		super( dataSource.getUri(), inputStream );
		this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
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

    private Variables getVariablesCache()
    {
        return this.variablesCache;
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
		if (reader.isStartElement())
		{
            String localName = reader.getLocalName();

            if ( localName.equalsIgnoreCase( "timeSeries" ) )
            {
                LOGGER.debug( "Read first element 'timeSeries' of {}",
                              this.getFilename() );

                if ( !this.traceValues.isEmpty() )
                {
                    LOGGER.debug( "Ingesting due to non-empty tracevalues" );
                    TimeSeries<?> timeSeries =
                            buildTimeSeries( this.currentTimeSeriesMetadata,
                                             this.traceValues,
                                             this.currentTraceName,
                                             reader.getLocation()
                                                   .getLineNumber() );
                    this.ingest( timeSeries );

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
     * @throws ProjectConfigException when a forecast is missing a forecast date
     * @throws PreIngestException When data is improperly formatted.
	 */

	private void parseEvent(XMLStreamReader reader)
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
		Duration scalePeriod = null;
		TimeScale.TimeScaleFunction scaleFunction = TimeScale.TimeScaleFunction.UNKNOWN;
		Duration timeStep = null;
        LocalDateTime forecastDate = null;
        String locationName = null;
        String variableName = null;
        String unitName = null;
        String traceName = DEFAULT_ENSEMBLE_NAME;
        String ensembleMemberId = null;
        String ensembleMemberIndex = null;
        Double missingValue = null;

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
				    // TODO: Set the LID on a FeatureDetails object; don't just store the LID
					//	If we are at the tag for the location id, save it to the location metadata
					locationName = XMLHelper.getXMLText( reader);

					if ( locationName.length() > 5 )
					{
					    String shortendID = locationName.substring(0, 5);
					    Features features = this.getFeaturesCache();

					    if ( features.lidExists( shortendID ) )
						{
							locationName = shortendID;
						}
					}
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
                        scalePeriod = Duration.ofMinutes( 1 );
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

		// See #59438
		// For accumulative data, the scalePeriod has not been set, and this is equal
		// to the timeStep
		if( Objects.isNull( scalePeriod ) )
		{
		    scalePeriod = timeStep;
		}

		TimeScale scale = TimeScale.of( scalePeriod, scaleFunction );

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

        TimeSeriesMetadata justParsed = TimeSeriesMetadata.of( basisDatetimes,
                                                               scale,
                                                               variableName,
                                                               locationName,
                                                               unitName );

        // If we encounter a new header, that means a previous timeseries trace
        // was actually a full trace and needs ingest.
        if ( !justParsed.equals( this.currentTimeSeriesMetadata )
             && !this.traceValues.isEmpty() )
        {
            LOGGER.debug( "Saving a trace as a standalone timeseries because {} not equal to {}",
                          justParsed, this.currentTimeSeriesMetadata );
            TimeSeries<?> timeSeries =
                    buildTimeSeries( this.currentTimeSeriesMetadata,
                                     this.traceValues,
                                     this.currentTraceName,
                                     this.highestLineNumber );
            this.ingest( timeSeries );
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
     * originating from the data source configuration. While parsing the data,
     * if this value is encountered, it indicates that the value should be
     * ignored as it represents invalid data. This should be ignored in data
     * sources that define their own missing value.
     */
    protected Double getSpecifiedMissingValue()
    {
        if (missingValue == null && this.getDataSourceConfig() != null)
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
			!value.equalsIgnoreCase( "null" ) &&
            this.getSpecifiedMissingValue() != null)
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
            TimeSeries<?> timeSeries =
                    buildTimeSeries( this.currentTimeSeriesMetadata,
                                     this.traceValues,
                                     this.currentTraceName,
                                     this.highestLineNumber );
            this.ingest( timeSeries );
        }

        this.completeIngest();
    }


    /**
     * Build a timeseries out of temporary data structures.
     *
     * When there is a placeholder reference datetime, replace it with the
     * latest valid datetime found as "latest observation." This means there was
     * no reference datetime found in the XML, but until the WRES db schema is
     * ready to store any kind of timeseries with 0, 1, or N reference datetimes
     * we are required to specify something here.
     *
     * @param lastTimeSeriesMetadata The metadata for most-recently-parsed data.
     * @param ensembleValues The most-recently-parsed data in sorted map form.
     * @param lastEnsembleName The most-recently-parsed ensemble name.
     * @param lineNumber The most-recently-parsed line number in the csv source.
     * @return A TimeSeries either of Double or Ensemble, ready for ingest.
     * @throws PreIngestException When something goes wrong.
     */
    private TimeSeries<?> buildTimeSeries( TimeSeriesMetadata lastTimeSeriesMetadata,
                                           SortedMap<String,SortedMap<Instant,Double>> ensembleValues,
                                           String lastEnsembleName,
                                           int lineNumber )
    {
        LOGGER.debug( "buildTimeSeries called with {}, {}, {}, {}",
                      lastTimeSeriesMetadata,
                      ensembleValues,
                      lastEnsembleName,
                      lineNumber );
        TimeSeries<?> timeSeries;
        TimeSeriesMetadata metadata;
        Collection<Instant> referenceDatetimes =
                lastTimeSeriesMetadata.getReferenceTimes()
                                      .values();

        // When there are no reference datetimes, use latest value
        // by valid datetime. (Eventually we should remove the
        // restriction of requiring a reference datetime when db is
        // ready for it to be relaxed)
        if ( referenceDatetimes.size() == 1
             && referenceDatetimes.contains( PLACEHOLDER_REFERENCE_DATETIME ) )
        {
            LOGGER.debug( "Found placeholder reference datetime in {}",
                          lastTimeSeriesMetadata);
            Instant latestDatetime = ensembleValues.get( lastEnsembleName )
                                                   .lastKey();
            metadata = TimeSeriesMetadata.of( Map.of( LATEST_OBSERVATION, latestDatetime ),
                                              lastTimeSeriesMetadata.getTimeScale(),
                                              lastTimeSeriesMetadata.getVariableName(),
                                              lastTimeSeriesMetadata.getFeatureName(),
                                              lastTimeSeriesMetadata.getUnit() );
        }
        else
        {
            LOGGER.debug( "Found NO placeholder reference datetime in {}",
                          lastTimeSeriesMetadata);
            metadata = lastTimeSeriesMetadata;
        }

        // Check if this is actually an ensemble or single trace
        if ( ensembleValues.size() == 1
             && ensembleValues.firstKey()
                              .equals( DEFAULT_ENSEMBLE_NAME ) )
        {
            timeSeries = this.transform( metadata,
                                         ensembleValues.get( DEFAULT_ENSEMBLE_NAME ),
                                         lineNumber );
        }
        else
        {
            timeSeries = this.transformEnsemble( metadata,
                                                 ensembleValues,
                                                 lineNumber );
        }

        LOGGER.debug( "transformed into {}", timeSeries );
        return timeSeries;
    }

    /**
     * Transform a single trace into a TimeSeries of doubles.
     * @param metadata The metadata of the timeseries.
     * @param trace The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @return The complete TimeSeries
     */

    private TimeSeries<Double> transform( TimeSeriesMetadata metadata,
                                          SortedMap<Instant,Double> trace,
                                          int lineNumber )
    {
        if ( trace.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot transform fewer than "
                                                + "one values into timeseries "
                                                + "with metadata "
                                                + metadata
                                                + " from line number "
                                                + lineNumber );
        }

        TimeSeries.TimeSeriesBuilder<Double> builder = new TimeSeries.TimeSeriesBuilder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant,Double> events : trace.entrySet() )
        {
            Event<Double> event = Event.of( events.getKey(), events.getValue() );
            builder.addEvent( event );
        }

        return builder.build();
    }

    /**
     * Transform a map of traces into a TimeSeries of ensembles (flip it) but
     * also validate the density and valid datetimes of the ensemble prior.
     * @param metadata The metadata of the timeseries.
     * @param traces The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @return The complete TimeSeries
     * @throws IllegalArgumentException When fewer than two traces given.
     * @throws PreIngestException When ragged (non-dense) data given.
     */

    private TimeSeries<Ensemble> transformEnsemble( TimeSeriesMetadata metadata,
                                                    SortedMap<String,SortedMap<Instant,Double>> traces,
                                                    int lineNumber )
    {
        int traceCount = traces.size();

        if ( traceCount < 2 )
        {
            LOGGER.debug( "Found 'ensemble' data with fewer than two traces: {}",
                          traces );
        }

        Map<Instant,double[]> reshapedValues = null;
        Map.Entry<String,SortedMap<Instant,Double>> previousTrace = null;
        int i = 0;

        for ( Map.Entry<String,SortedMap<Instant,Double>> trace : traces.entrySet() )
        {
            SortedSet<Instant> theseInstants = new TreeSet<>( trace.getValue()
                                                                   .keySet() );

            if ( Objects.nonNull( previousTrace ) )
            {
                SortedSet<Instant> previousInstants = new TreeSet<>( previousTrace.getValue()
                                                                                  .keySet() );
                if ( !theseInstants.equals( previousInstants ) )
                {
                    throw new PreIngestException( "Cannot build ensemble from "
                                                  + this.getDataSource()
                                                        .getUri()
                                                  + " with data at or before "
                                                  + "line number "
                                                  + lineNumber
                                                  + " because the trace named "
                                                  + trace.getKey()
                                                  + " had these valid datetimes"
                                                  + ": " + theseInstants
                                                  + " but previous trace named "
                                                  + previousTrace.getKey()
                                                  + " had different ones: "
                                                  + previousInstants
                                                  + " which is not allowed. All"
                                                  + " traces must be dense and "
                                                  + "match valid datetimes." );
                }
            }

            if ( Objects.isNull( reshapedValues ) )
            {
                reshapedValues = new HashMap<>( theseInstants.size() );
            }

            for ( Map.Entry<Instant,Double> event : trace.getValue()
                                                         .entrySet() )
            {
                Instant validDateTime = event.getKey();

                if ( !reshapedValues.containsKey( validDateTime ) )
                {
                    reshapedValues.put( validDateTime, new double[traceCount] );
                }

                double[] values = reshapedValues.get( validDateTime );
                values[i] = event.getValue();
            }

            previousTrace = trace;
            i++;
        }

        wres.datamodel.time.TimeSeries.TimeSeriesBuilder<Ensemble> builder =
                new wres.datamodel.time.TimeSeries.TimeSeriesBuilder<>();

        // Because the iteration is over a sorted map, assuming same order here.
        SortedSet<String> traceNamesSorted = new TreeSet<>( traces.keySet() );
        String[] traceNames = new String[traceNamesSorted.size()];
        traceNamesSorted.toArray( traceNames );

        builder.setMetadata( metadata );

        for ( Map.Entry<Instant,double[]> events : reshapedValues.entrySet() )
        {
            Ensemble ensembleSlice = Ensemble.of( events.getValue(), traceNames );
            Event<Ensemble> ensembleEvent = Event.of( events.getKey(), ensembleSlice );
            builder.addEvent( ensembleEvent );
        }

        return builder.build();
    }



    /**
     * Create an ingester for the given timeseries and ingest in current Thread.
     * @param timeSeries The timeSeries to ingest.
     * @throws IngestException When anything goes wrong related to ingest.
     */

    private void ingest( wres.datamodel.time.TimeSeries<?> timeSeries )
            throws IngestException
    {
        TimeSeriesIngester timeSeriesIngester =
                this.createTimeSeriesIngester( this.getSystemSettings(),
                                               this.getDatabase(),
                                               this.getFeaturesCache(),
                                               this.getVariablesCache(),
                                               this.getEnsemblesCache(),
                                               this.getMeasurementUnitsCache(),
                                               this.getProjectConfig(),
                                               this.getDataSource(),
                                               this.getLockManager(),
                                               timeSeries );
        try
        {
            List<IngestResult> ingestResults = timeSeriesIngester.call();
            this.ingested.addAll( ingestResults );
        }
        catch ( IOException ioe )
        {
            throw new IngestException( "Failed to ingest data from "
                                       + this.getFilename() + ":", ioe );
        }
    }

    private void completeIngest()
    {
        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Parsed and ingested {} timeseries from {}",
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
	 * The value which indicates a null or invalid value from the source
	 */
	private Double missingValue = null;

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
                                                 Variables variablesCache,
                                                 Ensembles ensemblesCache,
                                                 MeasurementUnits measurementUnitsCache,
                                                 ProjectConfig projectConfig,
                                                 DataSource dataSource,
                                                 DatabaseLockManager lockManager,
                                                 wres.datamodel.time.TimeSeries<?> timeSeries )
    {
        return TimeSeriesIngester.of( systemSettings,
                                      database,
                                      featuresCache,
                                      variablesCache,
                                      ensemblesCache,
                                      measurementUnitsCache,
                                      projectConfig,
                                      dataSource,
                                      lockManager,
                                      timeSeries,
                                      TimeSeriesIngester.GEO_ID_TYPE.LID );
    }
}

package wres.reading.fews;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;
import com.sun.xml.fastinfoset.stax.StAXDocumentParser; // NOSONAR

import wres.datamodel.types.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.PreReadException;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.system.xml.XMLHelper;

/**
 * Reads time-series data from a Published Interface XML source. Further information about the format can be found 
 * here:
 *
 * <p><a href="https://publicwiki.deltares.nl/display/FEWSDOC/The+Delft-Fews+Published+Interface">PI-XML</a> 
 *
 * <p>The above link was last accessed: 20220802T12:00Z.
 *
 * @author James Brown
 * @author Christopher Tubbs
 */
public final class PublishedInterfaceXmlReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PublishedInterfaceXmlReader.class );

    /** Default ensemble name. */
    private static final String DEFAULT_ENSEMBLE_NAME = "default";

    /** See "missVal" documentation at
     * <a href="http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd">pi-xml</a>. */
    private static final double PIXML_DEFAULT_MISSING_VALUE = Double.NaN;

    /** Header string re-used several times. */
    private static final String HEADER = "header";

    /** Column string re-used several times. */
    private static final String AND_COLUMN = " and column ";

    /** Default XML factory. */
    private static final XMLInputFactory DEFAULT_XML_FACTORY = XMLInputFactory.newFactory();

    /**
     * @return an instance
     */

    public static PublishedInterfaceXmlReader of()
    {
        return new PublishedInterfaceXmlReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource, false );

        try
        {
            Path xmlPath = Paths.get( dataSource.getUri() );
            InputStream inputStream = new BufferedInputStream( Files.newInputStream( xmlPath ) );
            return this.readFromStream( dataSource, inputStream );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a CSV source.", e );
        }
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        return this.readFromStream( dataSource, inputStream );
    }

    /**
     * Reads pi-xml data from a stream.
     * @param dataSource the data source
     * @param inputStream the data stream
     * @return the time-series streams
     */
    private Stream<TimeSeriesTuple> readFromStream( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource,
                                                 DataDisposition.XML_PI_TIMESERIES,
                                                 DataDisposition.XML_FI_TIMESERIES );

        XMLStreamReader xmlStreamReader = this.getXmlStreamReader( dataSource, inputStream );

        // Get the lazy supplier of time-series data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, xmlStreamReader );

        // Generate a stream of time-series. Nothing is read here. Rather, as part of a terminal operation on this 
        // stream, each pull will read through to the supplier, then in turn to the data provider, and finally to 
        // the data source.
        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     // Close the data provider when the stream is closed
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event, closing an underlying data provider." );

                         try
                         {
                             xmlStreamReader.close();
                         }
                         catch ( XMLStreamException e )
                         {
                             LOGGER.warn( "Unable to close an XML stream for data source {}.",
                                          dataSource.getUri() );
                         }
                     } );
    }

    /**
     * Returns a time-series supplier from the inputs.
     *
     * @param dataSource the data source
     * @param xmlStreamReader the xml stream reader
     * @return a time-series supplier
     * @throws ReadException if the data could not be read for any reason
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             XMLStreamReader xmlStreamReader )
    {
        // Map of traces against labels
        SortedMap<String, SortedMap<Instant, Double>> traceValues = new TreeMap<>();
        AtomicReference<TimeSeriesMetadata> lastTraceMetadata = new AtomicReference<>();
        AtomicReference<String> lastTraceName = new AtomicReference<>();
        AtomicReference<ZoneOffset> zoneOffset = new AtomicReference<>();

        // Was the final time-series returned already?
        AtomicBoolean returnedFinal = new AtomicBoolean();

        // Create a supplier that returns a time-series once complete
        return () -> {
            // New rows to increment
            try
            {
                while ( xmlStreamReader.hasNext() )
                {
                    // Increment the current series or return a completed one
                    // The series may contain up to N replicates
                    TimeSeriesTuple tuple = this.incrementOrCompleteSeries( xmlStreamReader,
                                                                            dataSource,
                                                                            traceValues,
                                                                            lastTraceName,
                                                                            lastTraceMetadata,
                                                                            zoneOffset );

                    // Complete? If so, return it
                    if ( Objects.nonNull( tuple ) )
                    {
                        return tuple;
                    }

                    // Increment
                    if ( xmlStreamReader.hasNext() )
                    {
                        xmlStreamReader.next();
                    }
                }
            }
            catch ( XMLStreamException e )
            {
                throw new ReadException( "While reading a PI-XML data source, " + dataSource.getUri() + ".", e );
            }

            // Create the only or final series, if it hasn't been created already
            if ( !returnedFinal.getAndSet( true ) )
            {
                return this.getTimeSeries( dataSource,
                                           lastTraceMetadata.get(),
                                           traceValues,
                                           lastTraceName.get(),
                                           xmlStreamReader.getLocation()
                                                          .getLineNumber() );
            }

            // Null sentinel to close stream
            return null;
        };
    }

    /**
     * @param dataSource the data source
     * @return a stream reader
     * @throws ReadException if the stream could not be created
     */
    private XMLStreamReader getXmlStreamReader( DataSource dataSource, InputStream inputStream )
    {
        // Fast-infoset encoded?
        if ( dataSource.getDisposition() == DataDisposition.XML_FI_TIMESERIES )
        {
            return new StAXDocumentParser( inputStream );
        }
        // Regular encoding
        else
        {
            try
            {
                return DEFAULT_XML_FACTORY.createXMLStreamReader( inputStream );
            }
            catch ( XMLStreamException e )
            {
                throw new ReadException( "While attempting to read " + dataSource.getUri() + ".", e );
            }
        }
    }

    /**
     * Adds the time-series data to the supplied map or cleans the map and returns a completed series.
     * @param reader the XML reader, positioned at a "series" tag
     * @param dataSource the data source
     * @param traceValues the trace values
     * @param currentTraceName the current trace name
     * @param currentTimeSeriesMetadata the current time-series metadata
     * @param zoneOffset the time zone offset
     * @return a time-series tuple or null
     */

    private TimeSeriesTuple incrementOrCompleteSeries( XMLStreamReader reader,
                                                       DataSource dataSource,
                                                       SortedMap<String, SortedMap<Instant, Double>> traceValues,
                                                       AtomicReference<String> currentTraceName,
                                                       AtomicReference<TimeSeriesMetadata> currentTimeSeriesMetadata,
                                                       AtomicReference<ZoneOffset> zoneOffset )
    {
        TimeSeriesTuple timeSeriesTuple = null;

        if ( reader.isStartElement() )
        {
            String localName = reader.getLocalName();

            if ( localName.equalsIgnoreCase( "timeSeries" ) )
            {
                LOGGER.debug( "Read first 'timeSeries' element of {}", dataSource.getUri() );

                if ( !traceValues.isEmpty() )
                {
                    LOGGER.debug( "Creating a time-series from the last structure due to non-empty trace values" );

                    timeSeriesTuple = this.getTimeSeries( dataSource,
                                                          currentTimeSeriesMetadata.get(),
                                                          traceValues,
                                                          currentTraceName.get(),
                                                          reader.getLocation()
                                                                .getLineNumber() );
                }
            }
            else if ( localName.equalsIgnoreCase( "timeZone" ) )
            {
                try
                {
                    ZoneOffset innerOffset = this.parseOffsetHours( reader );
                    zoneOffset.set( innerOffset );
                }
                catch ( XMLStreamException e )
                {
                    String message = "While reading the timeZone at line "
                                     + reader.getLocation().getLineNumber()
                                     + AND_COLUMN
                                     + reader.getLocation().getColumnNumber()
                                     + ", encountered an issue.";

                    throw new ReadException( message, e );
                }

                LOGGER.debug( "Read 'timeZone' element of {} as {}.", dataSource.getUri(), zoneOffset );
            }
            else if ( localName.equalsIgnoreCase( "series" ) )
            {
                try
                {
                    // This may or may not complete a series
                    timeSeriesTuple = this.parseSeries( reader,
                                                        dataSource,
                                                        zoneOffset.get(),
                                                        traceValues,
                                                        currentTraceName,
                                                        currentTimeSeriesMetadata );
                }
                catch ( XMLStreamException e )
                {
                    String message = "While reading a timeseries from " + dataSource
                                     + " at line "
                                     + reader.getLocation().getLineNumber()
                                     + AND_COLUMN
                                     + reader.getLocation().getColumnNumber()
                                     + ", encountered an issue.";
                    throw new ReadException( message, e );
                }
            }
        }

        return timeSeriesTuple;
    }

    /**
     * Parses offset hours from a reader that is positioned on "timeZone" tag.
     * <br />
     * Sets this reader's offset hours after parsing the value.
     * @param reader the reader positioned on the timeZone tag
     * @return the time zone offset
     * @throws XMLStreamException when underlying reader can't read next element
     * @throws NumberFormatException when the value cannot be parsed
     * @throws DateTimeException when the value is outside the range +/- 18 hrs
     */
    private ZoneOffset parseOffsetHours( XMLStreamReader reader )
            throws XMLStreamException
    {
        if ( reader.isStartElement()
             && reader.getLocalName()
                      .equalsIgnoreCase( "timeZone" ) )
        {
            reader.next();
        }
        String offsetValueText = reader.getText();
        double offsetHours = Double.parseDouble( offsetValueText );
        // There are timezones such as +8:45
        int offsetSeconds = ( int ) ( offsetHours * 3600.0 );
        return ZoneOffset.ofTotalSeconds( offsetSeconds );
    }

    /**
     * Interprets information within PIXML "series" tags and returns a series if complete.
     *
     * @param reader the XML reader, positioned at a "series" tag
     * @param dataSource the data source
     * @param zoneOffset the time zone offset
     * @param traceValues the trace values
     * @param currentTraceName the current trace name
     * @param currentTimeSeriesMetadata the current time-series metadata
     * @return a time-series or null
     */
    private TimeSeriesTuple parseSeries( XMLStreamReader reader,
                                         DataSource dataSource,
                                         ZoneOffset zoneOffset,
                                         SortedMap<String, SortedMap<Instant, Double>> traceValues,
                                         AtomicReference<String> currentTraceName,
                                         AtomicReference<TimeSeriesMetadata> currentTimeSeriesMetadata )
            throws XMLStreamException
    {
        AtomicDouble missingValue = new AtomicDouble( PIXML_DEFAULT_MISSING_VALUE );
        TimeSeriesTuple returnMe = null;

        // Identify the timezone offset. See #38801, superseded by #126661
        zoneOffset = this.getTimeZoneOffset( zoneOffset, dataSource );

        String localName;

        //  Loop through every element in a series (header -> entry -> entry -> ... )
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

                if ( localName.equalsIgnoreCase( HEADER ) )
                {
                    // This may complete an earlier time-series
                    returnMe = this.parseHeader( reader,
                                                 dataSource,
                                                 traceValues,
                                                 currentTraceName,
                                                 currentTimeSeriesMetadata,
                                                 zoneOffset,
                                                 missingValue );
                }
                else if ( localName.equalsIgnoreCase( "event" ) )
                {
                    this.parseEvent( reader,
                                     dataSource,
                                     traceValues,
                                     currentTraceName.get(),
                                     currentTimeSeriesMetadata.get(),
                                     zoneOffset,
                                     missingValue );
                }
            }
        }

        return returnMe;
    }

    /**
     * Returns the timezone offset from the ingested information and data source.
     * @param ingestedOffset the ingested timezone offset
     * @param dataSource the data source
     * @return the timezone offset
     */
    private ZoneOffset getTimeZoneOffset( ZoneOffset ingestedOffset, DataSource dataSource )
    {
        ZoneOffset returnMe = ingestedOffset;

        // See #38801, superseded by #126661
        if ( Objects.isNull( ingestedOffset ) )
        {
            // The offset may be declared for this source or for the overall dataset
            ZoneOffset offset = dataSource.getSource()
                                          .timeZoneOffset();

            // Overall offset for all sources?
            if ( Objects.isNull( offset ) )
            {
                offset = dataSource.getContext()
                                   .timeZoneOffset();
            }

            LOGGER.debug( "The declared 'time_zone_offset' for {} is {}.", dataSource.getSource(), offset );

            if ( Objects.isNull( offset ) )
            {
                String message = "While reading a PI-XML data source from '"
                                 + dataSource.getUri()
                                 + "', failed to identify a 'timeZone' in the time-series data and failed to discover "
                                 + "a 'time_zone_offset' in the project declaration. One of these is necessary to "
                                 + "correctly identify the time zone of the time-series data. Please add a "
                                 + "'time_zone_offset' to the project declaration for this individual data source or "
                                 + "for the overall dataset and try again.";
                throw new ReadException( message );
            }

            returnMe = offset;
        }
        else
        {
            ZoneOffset configuredOffset = dataSource.getSource()
                                                    .timeZoneOffset();
            if ( Objects.nonNull( configuredOffset )
                 && !configuredOffset.equals( ingestedOffset ) )
            {
                LOGGER.warn( "The declared 'time_zone_offset' for the data source at '{}' was {}, which does not match "
                             + "the 'timeZone' of {} for a time-series within the source. It is best not to declare "
                             + "the 'time_zone_offset' for a PI-XML source in the project declaration when the "
                             + "'timeZone' is available within the data source itself because the declaration will be "
                             + "ignored.",
                             dataSource.getUri(),
                             configuredOffset,
                             ingestedOffset );
            }
        }

        return returnMe;
    }

    /**
     * Updates the measurement unit in the time-series header.
     * @param header the existing header with ingested information
     * @param dataSource the data source
     */
    private void setMeasurementUnit( TimeSeriesHeader header, DataSource dataSource )
    {
        String finalUnit = header.unitName;

        // See #126661
        if ( Objects.isNull( header.unitName ) )
        {
            // The unit may be declared for this source or for the overall dataset
            String unit = dataSource.getSource()
                                    .unit();

            // Overall unit for all sources?
            if ( Objects.isNull( unit ) )
            {
                unit = dataSource.getContext()
                                 .unit();
            }

            LOGGER.debug( "The declared 'unit' for {} is {}.", dataSource.getSource(), unit );

            if ( Objects.isNull( unit ) )
            {
                String message = "While reading a PI-XML data source from '"
                                 + dataSource.getUri()
                                 + "', failed to identify the 'units' in the time-series data and failed to discover "
                                 + "a 'unit' in the project declaration. One of these is necessary to "
                                 + "correctly identify the measurement unit of the time-series data. Please add a "
                                 + "'unit' to the project declaration for this individual data source or "
                                 + "for the overall dataset and try again.";
                throw new ReadException( message );
            }

            finalUnit = unit;
        }
        else
        {
            String declaredUnit = dataSource.getSource()
                                            .unit();
            if ( Objects.nonNull( declaredUnit )
                 && !declaredUnit.equals( header.unitName ) )
            {
                LOGGER.warn( "The declared 'unit' for the data source at '{}' was {}, which does not match "
                             + "the 'units' of {} for a time-series within the source. It is best not to declare "
                             + "the 'unit' for a PI-XML source in the project declaration when the "
                             + "'units' is available within the data source itself because the declaration will be "
                             + "ignored.",
                             dataSource.getUri(),
                             declaredUnit,
                             header.unitName );
            }
        }

        header.unitName = finalUnit;
    }

    /**
     * Removes information about a measurement from an "event" tag. If a sufficient number of events have been
     * parsed, they are sent to the database to be saved.
     * @param reader The reader containing the current event tag
     * @param dataSource the data source
     * @param traceValues the trace values
     * @param currentTraceName the current trace name
     * @param currentTimeSeriesMetadata the current time-series metadata
     * @param zoneOffset the time zone offset
     * @param missingValue the missing value sentinel
     * @throws ReadException if the event could not be read
     * @throws PreReadException When data is improperly formatted.
     */

    private void parseEvent( XMLStreamReader reader,
                             DataSource dataSource,
                             SortedMap<String, SortedMap<Instant, Double>> traceValues,
                             String currentTraceName,
                             TimeSeriesMetadata currentTimeSeriesMetadata,
                             ZoneOffset zoneOffset,
                             AtomicDouble missingValue )
    {
        // #102285
        if ( Objects.isNull( currentTraceName ) )
        {
            throw new ReadException( "An event for metadata '"
                                     + currentTimeSeriesMetadata
                                     + "' in "
                                     + dataSource.getUri()
                                     + " did not have a trace name for the current trace. "
                                     + "Either the source is not properly formed or the "
                                     + "header was not read properly. Parsing cannot "
                                     + "continue." );
        }

        String value = "";
        String dateText = null;
        String timeText = null;

        for ( int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); attributeIndex++ )
        {
            String localName = reader.getAttributeLocalName( attributeIndex );

            if ( localName.equalsIgnoreCase( "value" ) )
            {
                value = reader.getAttributeValue( attributeIndex );
            }
            else if ( localName.equalsIgnoreCase( "date" ) )
            {
                dateText = reader.getAttributeValue( attributeIndex );
            }
            else if ( localName.equalsIgnoreCase( "time" ) )
            {
                timeText = reader.getAttributeValue( attributeIndex );
            }
        }

        LOGGER.trace( "Parsed an event: date={}, time={}, value={}.", dateText, timeText, value );

        if ( Objects.isNull( dateText ) || Objects.isNull( timeText ) )
        {
            throw new ReadException( "An event for " + currentTimeSeriesMetadata
                                     + " in "
                                     + dataSource.getUri()
                                     + " didn't have "
                                     + "information about when the value was valid. "
                                     + "The source is not properly formed and parsing "
                                     + "cannot continue." );
        }

        if ( value.isBlank() )
        {
            LOGGER.debug( "The event at {} {} in '{}' didn't have a value to save.",
                          dateText,
                          timeText,
                          dataSource.getUri() );
            return;
        }

        LocalDate localDate = LocalDate.parse( dateText );
        LocalTime localTime = LocalTime.parse( timeText );
        LocalDateTime dateTime = LocalDateTime.of( localDate, localTime );
        Instant fullDateTime = OffsetDateTime.of( dateTime, zoneOffset )
                                             .toInstant();

        SortedMap<Instant, Double> values = traceValues.get( currentTraceName );

        if ( Objects.isNull( values ) )
        {
            LOGGER.trace( "Creating new values because trace '{}' not found.",
                          currentTraceName );
            values = new TreeMap<>();
            traceValues.put( currentTraceName, values );
        }

        double numericValue = this.getValueToSave( value, missingValue.get() );
        LOGGER.trace( "About to save event at {} with value {} into values {}",
                      fullDateTime,
                      numericValue,
                      values );

        values.put( fullDateTime, numericValue );
    }

    /**
     * Interprets the information within PIXML "header" tags and returns a time-series if the last one is complete.
     * @param reader the reader positioned at the "header" tag
     * @param dataSource the data source
     * @param traceValues the trace values
     * @param currentTraceName the current trace name
     * @param currentTimeSeriesMetadata the current time-series metadata
     * @param zoneOffset the time zone offset
     * @param missingValue the missing value sentinel to update
     * @return a time-series or null
     * @throws XMLStreamException if the stream reading fails for any reason
     */

    private TimeSeriesTuple parseHeader( XMLStreamReader reader,
                                         DataSource dataSource,
                                         SortedMap<String, SortedMap<Instant, Double>> traceValues,
                                         AtomicReference<String> currentTraceName,
                                         AtomicReference<TimeSeriesMetadata> currentTimeSeriesMetadata,
                                         ZoneOffset zoneOffset,
                                         AtomicDouble missingValue )
            throws XMLStreamException
    {
        TimeSeriesHeader header = this.getTimeSeriesHeader( reader, dataSource );

        TimeScaleOuter scale = TimeScaleOuter.of( header.scalePeriod, header.scaleFunction );
        Map<ReferenceTimeType, Instant> referenceTimes = this.getReferenceTimesFromHeader( header, zoneOffset );
        String locationWkt = this.getWktFromHeader( header );
        Geometry geometry = MessageUtilities.getGeometry( header.locationName,
                                                          header.locationDescription,
                                                          null,
                                                          locationWkt );
        Feature feature = Feature.of( geometry );

        TimeSeriesMetadata justParsed = TimeSeriesMetadata.of( referenceTimes,
                                                               scale,
                                                               header.variableName,
                                                               feature,
                                                               header.unitName );

        TimeSeriesTuple returnMe = null;

        // If the metadata has changed, we have a new time-series. However, if the metadata has not changed, we have a
        // new time-series if there is no ensemble member label, just a default name
        boolean metadataEqual = justParsed.equals( currentTimeSeriesMetadata.get() );

        if ( ( !metadataEqual || traceValues.containsKey( DEFAULT_ENSEMBLE_NAME ) ) && !traceValues.isEmpty() )
        {
            if ( !metadataEqual && LOGGER.isDebugEnabled() )
            {
                LOGGER.debug(
                        "Saving a trace as a standalone time-series because the metadata has changed. The current "
                        + "metadata is {}. The new metadata is: {}.",
                        justParsed,
                        currentTimeSeriesMetadata );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Saving a trace as a standalone time-series even though the metadata has not changed. "
                              + "This occurs when there are multiple observation-like time-series with the same "
                              + "metadata per source. The new metadata is: {}.",
                              currentTimeSeriesMetadata );
            }

            returnMe = this.getTimeSeries( dataSource,
                                           currentTimeSeriesMetadata.get(),
                                           traceValues,
                                           currentTraceName.get(),
                                           reader.getLocation()
                                                 .getLineNumber() );

            traceValues.clear();
        }

        // Duplicate trace labels are not allowed in the same context/source: #110238
        // Check here to allow for the trace values to be cleared above if starting a new series
        if ( traceValues.containsKey( header.traceName ) )
        {
            throw new ReadException( "Found invalid data in PI-XML source '"
                                     + dataSource.getUri()
                                     + "' near line "
                                     + reader.getLocation().getLineNumber()
                                     + ": discovered two or more time-series with the same ensemble trace "
                                     + "identifier of '"
                                     + currentTraceName
                                     + "', which is not allowed." );
        }

        LOGGER.trace( "Setting the missing value sentinel to {}.", header.missValue );

        missingValue.set( header.missValue );
        currentTraceName.set( header.traceName );

        // Create new trace for each header in PI-XML data.
        currentTimeSeriesMetadata.set( justParsed );

        return returnMe;
    }

    /**
     * Sets the ensemble trace name in the header.
     * @param header the header that requires a trace name
     * @param reader the reader
     * @param dataSource the data source
     * @throws ReadException if the trace name could not be set due to an invalid header
     */

    private void setEnsembleTraceName( TimeSeriesHeader header,
                                       XMLStreamReader reader,
                                       DataSource dataSource )
    {
        if ( Objects.nonNull( header.ensembleMemberId )
             && Objects.nonNull( header.ensembleMemberIndex ) )
        {
            throw new ReadException( "Found invalid data in PI-XML source '"
                                     + dataSource.getUri()
                                     + "' near line "
                                     + reader.getLocation().getLineNumber()
                                     + ": a trace may contain either an ensembleMemberId or an "
                                     + "ensembleMemberIndex, but not both. Found ensembleMemberId"
                                     + " '"
                                     + header.ensembleMemberId
                                     + "' and ensembleMemberIndex of '"
                                     + header.ensembleMemberIndex
                                     + "'. For more details see "
                                     + "https://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" );
        }

        if ( Objects.nonNull( header.ensembleMemberId ) )
        {
            header.traceName = header.ensembleMemberId;
        }
        else if ( Objects.nonNull( header.ensembleMemberIndex ) )
        {
            header.traceName = header.ensembleMemberIndex;
        }
    }

    /**
     * Sets the location description.
     * @param header the time-series header
     */

    private void setLocationDescription( TimeSeriesHeader header )
    {
        if ( header.locationStationName != null )
        {
            header.locationDescription = header.locationStationName;
        }

        if ( header.locationLongName != null )
        {
            // Append the long name to description when already set with station
            if ( header.locationDescription != null )
            {
                header.locationDescription += " " + header.locationLongName;
            }
            else
            {
                header.locationDescription = header.locationLongName;
            }
        }
    }

    /**
     * Gets a time-series header from the reader.
     * @param reader the reader
     * @param dataSource the data source to help with messaging
     * @return the time-series header
     * @throws XMLStreamException if the stream could not be read
     */

    private TimeSeriesHeader getTimeSeriesHeader( XMLStreamReader reader, DataSource dataSource )
            throws XMLStreamException
    {
        TimeSeriesHeader header = new TimeSeriesHeader();

        //  If the current tag is the header tag itself, move on to the next tag
        if ( reader.isStartElement()
             && reader.getLocalName()
                      .equalsIgnoreCase( HEADER ) )
        {
            reader.next();
        }

        //  Scrape all pertinent information from the header
        while ( reader.hasNext() )
        {
            if ( reader.isEndElement()
                 && reader.getLocalName()
                          .equalsIgnoreCase( HEADER ) )
            {
                //  Leave the loop when we arrive at the end tag
                break;
            }
            else if ( reader.isStartElement() )
            {
                this.updateTimeSeriesHeader( reader, header, dataSource );
            }

            reader.next();
        }

        // Update the unit if declared rather than supplied inband
        this.setMeasurementUnit( header, dataSource );

        // Validate and set the unique trace name
        this.setEnsembleTraceName( header, reader, dataSource );

        // Set the location description
        this.setLocationDescription( header );

        // See #59438
        // For accumulative data, the scalePeriod has not been set, and this is equal
        // to the timeStep, if available
        if ( Objects.isNull( header.scalePeriod ) )
        {
            header.scalePeriod = header.timeStep;
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Parsed PI-XML header: scalePeriod={}, scaleFunction={}, timeStep={}, forecastDate={}, "
                          + "locationName={}, variableName={}, unitName={}, traceName={}, ensembleMemberId={}, "
                          + "ensembleMemberIndex={}, missingValue={}, locationLongName={}, locationStationName={}, "
                          + "locationDescription={}, latitude={}, longitude={}, x={}, y={}, z={}.",
                          header.scalePeriod,
                          header.scaleFunction,
                          header.timeStep,
                          header.forecastDate,
                          header.locationName,
                          header.variableName,
                          header.unitName,
                          header.traceName,
                          header.ensembleMemberId,
                          header.ensembleMemberIndex,
                          header.missValue,
                          header.locationLongName,
                          header.locationStationName,
                          header.locationDescription,
                          header.latitude,
                          header.longitude,
                          header.x,
                          header.y,
                          header.z );
        }

        return header;
    }

    /**
     * Updates the time-series header
     * @param reader the reader
     * @param h the header
     * @param dataSource the data source to help with error messaging
     * @throws XMLStreamException if the stream could not be read
     */

    private void updateTimeSeriesHeader( XMLStreamReader reader,
                                         TimeSeriesHeader h,
                                         DataSource dataSource )
            throws XMLStreamException
    {
        String localName = reader.getLocalName();

        if ( localName.equalsIgnoreCase( "locationId" ) )
        {
            // Change for 5.0: just store location verbatim. No magic.
            h.locationName = XMLHelper.getXMLText( reader );
        }
        else if ( localName.equalsIgnoreCase( "units" ) )
        {
            h.unitName = XMLHelper.getXMLText( reader );
        }
        else if ( localName.equalsIgnoreCase( "missVal" ) )
        {
            // If we are at the tag for the missing value definition, record it
            h.missValue = Double.parseDouble( XMLHelper.getXMLText( reader ) );
        }
        else if ( localName.equalsIgnoreCase( "parameterId" ) )
        {
            h.variableName = XMLHelper.getXMLText( reader );
        }
        else if ( localName.equalsIgnoreCase( "forecastDate" ) )
        {
            h.forecastDate = PublishedInterfaceXmlReader.parseDateTime( reader );
        }
        else if ( localName.equalsIgnoreCase( "type" )
                  && XMLHelper.getXMLText( reader )
                              .equalsIgnoreCase( "instantaneous" ) )
        {
            // See #59438
            h.scalePeriod = Duration.ofMillis( 1 );
        }
        else if ( localName.equalsIgnoreCase( "timeStep" ) )
        {
            String unit = XMLHelper.getAttributeValue( reader, "unit" );
            unit = unit.toUpperCase();
            String multiplier = XMLHelper.getAttributeValue( reader, "multiplier" );
            h.timeStep = this.getTimeStep( unit, multiplier, dataSource );
        }
        else if ( localName.equalsIgnoreCase( "ensembleMemberId" ) )
        {
            h.ensembleMemberId = XMLHelper.getXMLText( reader );
        }
        else if ( localName.equalsIgnoreCase( "ensembleMemberIndex" ) )
        {
            h.ensembleMemberIndex = XMLHelper.getXMLText( reader );
        }
        else if ( localName.equalsIgnoreCase( "longName" ) )
        {
            h.locationLongName = XMLHelper.getXMLText( reader );
        }
        else if ( localName.equalsIgnoreCase( "stationName" ) )
        {
            h.locationStationName = XMLHelper.getXMLText( reader );
        }

        // Add the georeferencing attributes
        this.updateHeaderWithGeoreferencing( reader, h, localName );
    }

    /**
     * Parses the time-step from a unit and multiplier.
     * @param unit the unit
     * @param multiplier the multiplier, possibly null
     * @param dataSource the data source to help with error messaging
     * @return the time-step
     * @throws ReadException if the unit and/or multiplier are not correctly formatted
     */

    private Duration getTimeStep( String unit, String multiplier, DataSource dataSource )
    {
        if ( unit.equalsIgnoreCase( "nonequidistant" ) )
        {
            LOGGER.debug( "Discovered a PI-XML data-source with an irregular time-series, i.e., a "
                          + "nonequidistant time-step unit. " );

            return null;
        }

        String unitString = unit + "S";

        try
        {
            ChronoUnit chronoUnit = ChronoUnit.valueOf( unitString );
            int amount = Integer.parseInt( multiplier );
            return Duration.of( amount, chronoUnit );
        }
        catch ( NumberFormatException e )
        {
            throw new ReadException( "While attempting to read a data source with URI "
                                     + dataSource.getUri()
                                     + ", could not parse the value of the timeStep multiplier associated with one or "
                                     + "more time-series. The multiplier should be an integer, but was: '"
                                     + multiplier
                                     + "'. The timeStep unit was: '"
                                     + unit
                                     + "'." );
        }
        catch ( IllegalArgumentException e )
        {
            throw new ReadException( "While attempting to read a data source with URI "
                                     + dataSource.getUri()
                                     + ", could not parse the value of the timeStep unit associated with one or "
                                     + "more time-series. The unit was: '"
                                     + unit
                                     + "'. An 'S' was appended to this unit string and interpreted as '"
                                     + unitString
                                     + "'." );
        }
    }

    /**
     * Updates the time-series header with georeferencing attributes.
     * @param reader the reader
     * @param h the header
     * @param localName the name of the attribute to read
     * @throws XMLStreamException if the stream could not be read
     */

    private void updateHeaderWithGeoreferencing( XMLStreamReader reader,
                                                 TimeSeriesHeader h,
                                                 String localName )
            throws XMLStreamException
    {
        if ( localName.equalsIgnoreCase( "lat" ) )
        {
            String rawLatitude = XMLHelper.getXMLText( reader );
            h.latitude = Double.parseDouble( rawLatitude );
        }
        else if ( localName.equalsIgnoreCase( "lon" ) )
        {
            String rawLongitude = XMLHelper.getXMLText( reader );
            h.longitude = Double.parseDouble( rawLongitude );
        }

        else if ( localName.equalsIgnoreCase( "x" ) )
        {
            String rawX = XMLHelper.getXMLText( reader );
            h.x = Double.parseDouble( rawX );
        }
        else if ( localName.equalsIgnoreCase( "y" ) )
        {
            String rawY = XMLHelper.getXMLText( reader );
            h.y = Double.parseDouble( rawY );
        }
        else if ( localName.equalsIgnoreCase( "z" ) )
        {
            String rawZ = XMLHelper.getXMLText( reader );
            h.z = Double.parseDouble( rawZ );
        }
    }

    /**
     * Creates a wkt from the time-series header.
     * @param header the header
     * @return a wkt string
     */
    private String getWktFromHeader( TimeSeriesHeader header )
    {
        String locationWkt = null;

        // When x and y are present, prefer those to lon, lat.
        // Going to Double back to String seems frivolous, but it validates data.
        if ( Objects.nonNull( header.x )
             && Objects.nonNull( header.y ) )
        {
            StringJoiner wktGeometry = new StringJoiner( " " );
            wktGeometry.add( "POINT (" );
            wktGeometry.add( header.x.toString() );
            wktGeometry.add( header.y.toString() );

            if ( Objects.nonNull( header.z ) )
            {
                wktGeometry.add( header.z.toString() );
            }
            wktGeometry.add( ")" );
            locationWkt = wktGeometry.toString();
        }
        else if ( Objects.nonNull( header.latitude ) && Objects.nonNull( header.longitude ) )
        {
            StringJoiner wktGeometry = new StringJoiner( " " );
            wktGeometry.add( "POINT (" );
            wktGeometry.add( header.longitude.toString() );
            wktGeometry.add( header.latitude.toString() );
            wktGeometry.add( ")" );
            locationWkt = wktGeometry.toString();
        }

        return locationWkt;
    }

    /**
     * Creates a map of reference times from the header.
     * @param header the header
     * @param zoneOffset the time zone offset
     * @return a map of reference times
     */
    private Map<ReferenceTimeType, Instant> getReferenceTimesFromHeader( TimeSeriesHeader header,
                                                                         ZoneOffset zoneOffset )
    {
        Map<ReferenceTimeType, Instant> referenceTimes = new EnumMap<>( ReferenceTimeType.class );

        if ( Objects.nonNull( header.forecastDate ) )
        {
            Instant t0 = OffsetDateTime.of( header.forecastDate, zoneOffset )
                                       .toInstant();
            referenceTimes.put( ReferenceTimeType.T0, t0 );
        }

        return Collections.unmodifiableMap( referenceTimes );
    }

    /**
     * Reads the date and time from an XML reader that stores the date and time in separate attributes
     * @param reader The XML Reader positioned at a node containing date and time attributes
     * @return a LocalDateTime representing the parsed value
     * @throws ReadException if the datetime could not be parsed
     */
    private static LocalDateTime parseDateTime( XMLStreamReader reader )
    {
        LocalDate date = null;
        LocalTime time = null;
        String localName;

        for ( int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex )
        {
            localName = reader.getAttributeLocalName( attributeIndex );

            if ( localName.equalsIgnoreCase( "date" ) )
            {
                String dateText = reader.getAttributeValue( attributeIndex );
                date = LocalDate.parse( dateText );
            }
            else if ( localName.equalsIgnoreCase( "time" ) )
            {
                String timeText = reader.getAttributeValue( attributeIndex );
                time = LocalTime.parse( timeText );
            }
        }

        if ( date == null || time == null )
        {
            throw new ReadException( "Could not parse date and time at line "
                                     + reader.getLocation().getLineNumber()
                                     + " column "
                                     + reader.getLocation().getColumnNumber() );
        }

        return LocalDateTime.of( date, time );
    }

    /**
     * Conditions the passed in value and transforms it into a form suitable to
     * save into the database.
     * <p>
     *     If the passed in value is found to be equal to the specified missing
     *     value, it is set to WRES' Missing Value
     * </p>
     * @param value the original value
     * @param missingValue the missing value identifier in the stream
     * @return The conditioned value that is safe to save to the database.
     */
    private double getValueToSave( String value, double missingValue )
    {
        if ( Objects.isNull( value )
             || value.isBlank()
             || value.equalsIgnoreCase( "null" ) )
        {
            return MissingValues.DOUBLE;
        }

        value = value.strip();
        double val = Double.parseDouble( value );

        if ( Precision.equals( val, missingValue, Precision.EPSILON ) )
        {
            return MissingValues.DOUBLE;
        }

        return val;
    }

    /**
     * Build a timeseries out of temporary data structures.
     *
     * @param dataSource the data source
     * @param timeSeriesMetadata The metadata for most-recently-parsed data.
     * @param ensembleValues The most-recently-parsed data in sorted map form.
     * @param lastEnsembleName The most-recently-parsed ensemble name.
     * @param lineNumber The most-recently-parsed line number in the csv source.
     * @return a tuple of time-series.
     */

    private TimeSeriesTuple getTimeSeries( DataSource dataSource,
                                           TimeSeriesMetadata timeSeriesMetadata,
                                           SortedMap<String, SortedMap<Instant, Double>> ensembleValues,
                                           String lastEnsembleName,
                                           int lineNumber )
    {
        LOGGER.debug( "Creating a time-series with metadata {}, ensemble values {}, last ensemble name {}, and line "
                      + "number {}.",
                      timeSeriesMetadata,
                      ensembleValues,
                      lastEnsembleName,
                      lineNumber );

        // Check if this is actually an ensemble or a single trace
        // Treat a one-member ensemble as single-valued and log
        TimeSeries<Double> singleValuedSeries = null;
        TimeSeries<Ensemble> ensembleSeries = null;

        if ( ensembleValues.size() == 1
             && ( ensembleValues.firstKey()
                                .equals( DEFAULT_ENSEMBLE_NAME )
                  || ensembleValues.containsKey( lastEnsembleName ) ) )
        {
            String name = DEFAULT_ENSEMBLE_NAME;
            if ( ensembleValues.containsKey( lastEnsembleName ) )
            {
                LOGGER.debug( "While reading time-series data in PI-XML format discovered a single trace with an "
                              + "ensemble member name of {}. Treating this as a single-valued time-series.",
                              lastEnsembleName );
                name = lastEnsembleName;
            }

            singleValuedSeries = ReaderUtilities.transform( timeSeriesMetadata,
                                                            ensembleValues.get( name ),
                                                            lineNumber,
                                                            dataSource.getUri() );


            // Validate
            ReaderUtilities.validateAgainstEmptyTimeSeries( singleValuedSeries, dataSource.getUri() );
        }
        else
        {
            ensembleSeries = ReaderUtilities.transformEnsemble( timeSeriesMetadata,
                                                                ensembleValues,
                                                                lineNumber,
                                                                dataSource.getUri() );

            // Validate
            ReaderUtilities.validateAgainstEmptyTimeSeries( ensembleSeries, dataSource.getUri() );
        }

        return TimeSeriesTuple.of( singleValuedSeries, ensembleSeries, dataSource );
    }

    /**
     * Hidden constructor.
     */

    private PublishedInterfaceXmlReader()
    {
    }

    /**
     * A value class that corresponds to the time-series header in pi-xml world. Do not expose. Ideally, this would be 
     * an immutable class, built incrementally, even though it is not exposed beyond this class, but this is a massive 
     * faff for limited benefit until we have automated builder creation. The reason for this class is to allow for 
     * header creation to be broken up into a smaller number of manageable methods, rather than one enormous method.
     *
     * @author James Brown
     */

    private static class TimeSeriesHeader
    {
        private Duration scalePeriod = null;
        private final TimeScaleFunction scaleFunction = TimeScaleFunction.UNKNOWN;
        private Duration timeStep = null;
        private LocalDateTime forecastDate = null;
        private String locationName = null;
        private String variableName = null;
        private String unitName = null;
        private String traceName = DEFAULT_ENSEMBLE_NAME;
        private String ensembleMemberId = null;
        private String ensembleMemberIndex = null;
        private double missValue = PIXML_DEFAULT_MISSING_VALUE;
        private String locationLongName = null;
        private String locationStationName = null;
        private String locationDescription = null;
        private Double latitude = null;
        private Double longitude = null;
        private Double x = null;
        private Double y = null;
        private Double z = null;

        private TimeSeriesHeader()
        {
        }
    }

}

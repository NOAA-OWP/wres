package wres.reading.wrds.nwm;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.types.Ensemble;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime;

/**
 * <p>Reads time-series data from the U.S. National Weather Service (NWS) National Water Model (NWM) supplied in a JSON
 * time-series format defined by the NWS Water Resources Data Service (WRDS).
 *
 * <p>Reading is performed incrementally using the Jackson streaming API ({@link JsonParser}). A single time-series
 * (one array element of the underlying JSON payload) is materialized and converted to a {@link TimeSeriesTuple} per
 * pull of the {@link Supplier} returned by {@link #getTimeSeriesSupplier(DataSource, JsonParser)}. This mirrors the
 * pull-based shape of the {@link Stream} returned to callers: nothing is read from the underlying source until a
 * terminal stream operation requests it, and only one time-series is materialized in memory at a time.
 *
 * @author James Brown
 */

public class WrdsNwmJsonReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmJsonReader.class );

    /** The forecast node identifier. **/
    private static final String FORECAST_NODE_FIELD = "forecast";

    /** Maps JSON bytes to POJOs/trees. */
    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder()
                      .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true )
                      .build();

    /**
     * @return an instance
     */

    public static WrdsNwmJsonReader of()
    {
        return new WrdsNwmJsonReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource, false );

        try
        {
            Path path = Paths.get( dataSource.uri() );
            InputStream stream = new BufferedInputStream( Files.newInputStream( path ) );
            return this.read( dataSource, stream );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a National Water Model Json source.", e );
        }
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.JSON_WRDS_NWM );

        // Open the streaming parser once. It owns the underlying InputStream from here on: closing the parser
        // closes the stream too (JsonParser.Feature.AUTO_CLOSE_SOURCE is enabled by default), so we don't need to
        // (and shouldn't) close inputStream separately below.
        JsonParser parser;
        try
        {
            parser = OBJECT_MAPPER.createParser( inputStream );
        }
        catch ( JacksonException e )
        {
            throw new ReadException( "Failed to open a streaming parser for a National Water Model Json source.",
                                     e );
        }

        // Get the lazy supplier of time-series data, one at a time
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, parser );

        // Generate a stream of time-series.
        // Each pull advances the underlying JsonParser by exactly one time-series worth of JSON, so a terminal
        // stream operation drives one incremental parse per element rather than an eager, whole-payload read.
        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     // Close the parser (and therefore the underlying stream) when the stream is closed
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event, closing an underlying JSON parser." );

                         try
                         {
                             parser.close();
                         }
                         catch ( RuntimeException e )
                         {
                             LOGGER.warn( "Unable to close a streaming parser for data source {}.",
                                          dataSource.uri() );
                         }
                     } );
    }

    /**
     * <p>Returns a supplier that, on each invocation, advances the supplied {@link JsonParser} past exactly one
     * time-series element of the underlying JSON array and returns it as a {@link TimeSeriesTuple}. Returns
     * {@code null} once the array is exhausted, which is the sentinel {@link Stream#generate(Supplier)} +
     * {@link Stream#takeWhile(java.util.function.Predicate)} uses to terminate the stream.
     *
     * <p>Navigation to the start of the time-series array happens lazily, on the first pull, so that no reading of
     * any kind happens until a terminal stream operation actually requests an element (consistent with the rest of
     * this reader's laziness).
     *
     * @param dataSource the data source, retained for error messages and for use by the tuple conversion
     * @param parser the streaming parser, positioned at the very start of the document on first use
     * @return a time-series supplier
     * @throws ReadException if the data could not be read for any reason
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             JsonParser parser )
    {
        final AtomicBoolean initialized = new AtomicBoolean( false );

        return () -> {
            try
            {
                // 1. Initialize root array once on first pull
                if ( !initialized.get() )
                {
                    JsonToken rootToken = parser.nextToken();
                    if ( rootToken != JsonToken.START_ARRAY )
                    {
                        throw new ReadException( "Expected a root JSON array, but found: " + rootToken );
                    }
                    initialized.set( true );
                }

                // 2. Step to the next element token in the root array
                JsonToken nextOuterToken = parser.nextToken();

                if ( nextOuterToken == JsonToken.END_ARRAY || Objects.isNull( nextOuterToken ) )
                {
                    LOGGER.debug( "Reached the end of the root array for data source {}.", dataSource.uri() );
                    return null;
                }

                if ( nextOuterToken != JsonToken.START_OBJECT )
                {
                    throw new ReadException( "Expected a JSON object inside root array, but found: " + nextOuterToken );
                }

                // 3. Capture the full parent object tree containing metadata and forecast arrays
                JsonNode fullFeatureNode = parser.readValueAsTree();

                // 4. Validate and map the data
                return this.processFeatureNode( fullFeatureNode, dataSource );
            }
            catch ( JacksonException e )
            {
                throw new ReadException( "Failed to read a time-series from source: " + dataSource.uri(), e );
            }
        };
    }

    /**
     * Validates the structural presence of the forecast field and delegates to conversion.
     *
     * @param featureNode the full JSON object node containing metadata and forecasts
     * @param dataSource the source configuration context
     * @return a completed TimeSeriesTuple
     */
    private TimeSeriesTuple processFeatureNode( JsonNode featureNode, DataSource dataSource )
    {
        if ( !featureNode.has( FORECAST_NODE_FIELD ) )
        {
            throw new ReadException( "Could not locate a '" + FORECAST_NODE_FIELD + "' field in object." );
        }

        return this.toTimeSeriesTuple( featureNode, dataSource );
    }

    /**
     * Converts one JSON array element (a single time-series, still as a raw {@link JsonNode}) into a
     * {@link TimeSeriesTuple}.
     *
     * @param forecastNode one forecast node
     * @param dataSource the data source, for metadata/provenance
     * @return the corresponding time-series tuple
     */

    private TimeSeriesTuple toTimeSeriesTuple( JsonNode forecastNode, DataSource dataSource )
    {
        try
        {
            URI uri = dataSource.uri();

            WrdsNwmForecastRecord forecastRecord = OBJECT_MAPPER.treeToValue( forecastNode,
                                                                              WrdsNwmForecastRecord.class );

            ReferenceTime.ReferenceTimeType referenceTimeType =
                    this.getReferenceTimeTypeFromConfiguration( forecastRecord.configuration() );

            String locationId = Long.toString( forecastRecord.nwmFeatureId() );
            Geometry geometry = MessageUtilities.getGeometry( locationId );
            Feature feature = Feature.of( geometry );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( referenceTimeType,
                                                                         forecastRecord.referenceDatetime() ),
                                                                 TimeScaleOuter.of(),
                                                                 forecastRecord.dataType(),
                                                                 feature,
                                                                 forecastRecord.units() );

            // Read into an intermediate structure
            SortedMap<String, SortedMap<Instant, Double>> traces = this.getTracesFromForecast( forecastRecord );

            // Single-valued
            if ( traces.size() == 1 )
            {
                TimeSeries<Double> series = ReaderUtilities.transform( metadata,
                                                                       traces.values()
                                                                             .iterator()
                                                                             .next(),
                                                                       -1,
                                                                       uri );

                LOGGER.debug( "Read a single-valued NWM time-series from the root document, which contained {} events.",
                              series.getEvents()
                                    .size() );

                // Validate
                ReaderUtilities.validateAgainstEmptyTimeSeries( series, dataSource.uri() );

                return TimeSeriesTuple.ofSingleValued( series, dataSource );
            }
            // Ensemble
            else if ( traces.size() > 1 )
            {
                TimeSeries<Ensemble> series = ReaderUtilities.transformEnsemble( metadata, traces );

                LOGGER.debug( "Read an ensemble NWM time-series from the root document, which contained {} events.",
                              series.getEvents()
                                    .size() );

                // Validate series
                ReaderUtilities.validateAgainstEmptyTimeSeries( series, dataSource.uri() );

                return TimeSeriesTuple.ofEnsemble( series, dataSource );
            }
            // No members
            else
            {
                throw new ReadException(
                        "While attempting to read NWM time-series data in WRDS JSON format for feature "
                        + locationId
                        + ", encountered zero traces, which is not allowed." );
            }
        }
        catch ( JacksonException e )
        {
            throw new ReadException( "Failed to parse a forecast record into a time-series domain object when reading "
                                     + "from data source: " + dataSource );
        }
    }

    /**
     * Returns the {@link ReferenceTime.ReferenceTimeType} from the configuration name.
     * @param configuration the configuration name
     * @return the reference time type
     */

    private ReferenceTime.ReferenceTimeType getReferenceTimeTypeFromConfiguration( String configuration )
    {
        if ( configuration.equalsIgnoreCase( "analysis_assim" ) )
        {
            LOGGER.debug( "Detected 'analysis_assim' configuration, so returning a reference time type of {}",
                          ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME );

            return ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME;
        }

        return ReferenceTime.ReferenceTimeType.T0;
    }

    /**
     * Converts a forecast record into a map of traces for onward mapping.
     *
     * @param forecast the deserialized forecast
     * @return a sorted map of member traces containing sorted time-series points
     */
    private SortedMap<String, SortedMap<Instant, Double>> getTracesFromForecast( WrdsNwmForecastRecord forecast )
    {
        SortedMap<String, SortedMap<Instant, Double>> traces = new TreeMap<>();

        for ( WrdsNwmForecastMember member : forecast.members() )
        {
            String memberKey = String.valueOf( member.memberId() );

            // Ensure a sorted map exists for this specific member trace
            SortedMap<Instant, Double> timeSeriesMap = traces.computeIfAbsent( memberKey, k -> new TreeMap<>() );

            for ( WrdsNwmForecastEvent dataPoint : member.events() )
            {
                timeSeriesMap.put( dataPoint.validDatetime(), dataPoint.value() );
            }
        }

        return Collections.unmodifiableSortedMap( traces );
    }

    /**
     * Hidden constructor.
     */

    private WrdsNwmJsonReader()
    {
    }
}
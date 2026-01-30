package wres.reading.nwis.ogc.response;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.Duration;
import org.apache.commons.io.IOUtils;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.DoubleEvent;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.reading.nwis.ogc.LocationMetadata;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale;

/**
 * <p>Reads time-series data responses from the National Water Information System (NWIS) Open Geospatial Consortium
 * (OGC) web services. Supported responses include daily values and instantaneous or continuous values.
 *
 * <p>Implementation notes:
 *
 * <p>This implementation simply reads the responses from a file source or supplied stream and does not interact with
 * the web service that originally supplied the responses. In other words, any contextual information that is required
 * at read time should be supplied on construction. This includes any information that is obtained from a separate
 * endpoint of the web API, which could be provided as a deferred function call (i.e., {@link Supplier}), for example.
 * This reader is (optionally) embellished with location metadata. However, when the location metadata is absent, the
 * reader will except at read time if the time zone information contained within that metadata is required to convert
 * local times to standard time (UTC).
 *
 * @author James Brown
 */
public class NwisResponseReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwisResponseReader.class );

    /** Maps GeoJSON bytes to POJOs. */
    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder()
                      .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true )
                      .build();

    /** Cache of location metadata. */
    private final Cache<@NonNull String, LocationMetadata> locationMetadata;

    /**
     * Creates an instance.
     *
     * @return an instance
     */

    public static NwisResponseReader of()
    {
        Cache<@NonNull String, LocationMetadata> metadata = Caffeine.newBuilder()
                                                                    .build();
        return new NwisResponseReader( metadata );
    }

    /**
     * Creates an instance with a cache of location metadata.
     *
     * @param locationMetadata the location metadata
     * @return an instance
     */

    public static NwisResponseReader of( Cache<@NonNull String, LocationMetadata> locationMetadata )
    {
        return new NwisResponseReader( locationMetadata );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getVariable() );
        Objects.requireNonNull( dataSource.getVariable()
                                          .name() );

        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource, false );

        try
        {
            Path path = Paths.get( dataSource.uri() );
            InputStream stream = Files.newInputStream( path );
            return this.readFromStream( dataSource, stream );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a GeoJSON data source formatted for the USGS National Water "
                                     + "Information System.", e );
        }
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        return this.readFromStream( dataSource, inputStream );
    }

    /**
     * Reads time-series data from a stream.
     * @param dataSource the data source
     * @param inputStream the data stream
     * @return the time-series streams
     * @throws NullPointerException if either input is null
     */
    private Stream<TimeSeriesTuple> readFromStream( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getVariable() );
        Objects.requireNonNull( dataSource.getVariable()
                                          .name() );
        Objects.requireNonNull( inputStream );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataSource.DataDisposition.GEOJSON );

        // Get the lazy supplier of time-series data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, inputStream );

        // Generate a stream of time-series.
        // This is merely a facade on incremental reading until the underlying supplier reads incrementally
        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     // Close the data provider when the stream is closed
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event, closing the underlying input stream." );

                         try
                         {
                             inputStream.close();
                         }
                         catch ( IOException e )
                         {
                             LOGGER.warn( "Unable to close a stream for data source {}.",
                                          dataSource.uri() );
                         }
                     } );
    }

    /**
     * <p>Returns a time-series supplier from the inputs. Currently, this method performs eager reading.
     *
     * @param dataSource the data source
     * @param inputStream the stream to read
     * @return a time-series supplier
     * @throws ReadException if the data could not be read for any reason
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             InputStream inputStream )
    {
        AtomicInteger iterator = new AtomicInteger();
        AtomicReference<List<TimeSeriesTuple>> timeSeriesTuples = new AtomicReference<>();

        // Create a supplier that returns the time-series
        return () -> {

            // Read all the time-series eagerly on first use: this will still delay any read until a terminal stream
            // operation pulls from the supplier (which is why we use a reference holder and do not request the
            // time-series outside of this lambda), but it will then acquire all the time-series eagerly, i.e., now
            if ( Objects.isNull( timeSeriesTuples.get() ) )
            {
                List<TimeSeriesTuple> eagerSeries = this.getTimeSeries( dataSource, inputStream );
                timeSeriesTuples.set( eagerSeries );
            }

            List<TimeSeriesTuple> tuples = timeSeriesTuples.get();

            // More time-series to return?
            if ( iterator.get() < tuples.size() )
            {
                return tuples.get( iterator.getAndIncrement() );
            }

            // Null sentinel to close stream
            return null;
        };
    }

    /**
     * Returns the time-series from the inputs.
     *
     * @param dataSource the data source
     * @param inputStream the stream to read
     * @return a time-series supplier
     * @throws ReadException if the data could not be read for any reason
     */

    private List<TimeSeriesTuple> getTimeSeries( DataSource dataSource,
                                                 InputStream inputStream )
    {
        try
        {
            // Get the bytes using a buffer
            byte[] rawForecast = IOUtils.toByteArray( inputStream );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Time-series from {} as UTF-8: {}",
                              dataSource.uri(),
                              new String( rawForecast,
                                          StandardCharsets.UTF_8 ) );
            }

            Response response = OBJECT_MAPPER.readValue( rawForecast, Response.class );

            List<TimeSeriesTuple> allTimeSeries = new ArrayList<>();

            // Some time-series with data
            if ( response.getNumberReturned() > 0 )
            {
                Map<String, List<wres.reading.nwis.ogc.response.Feature>> bySeries =
                        Arrays.stream( response.getFeatures() )
                              .collect( Collectors.groupingBy( f -> f.getProperties()
                                                                     .getTimeSeriesId() ) );
                for ( List<wres.reading.nwis.ogc.response.Feature> nextValues : bySeries.values() )
                {
                    TimeSeriesTuple nextSeries = this.transform( dataSource, nextValues, response.getLinks() );
                    allTimeSeries.add( nextSeries );
                }
            }

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Read {} time series from {}.",
                              allTimeSeries.size(),
                              dataSource.uri() );
            }

            return Collections.unmodifiableList( allTimeSeries );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a GeoJSON data stream formatted for the USGS National Water "
                                     + "Information System.", e );
        }
    }

    /**
     * Transforms a GeoJSON time-series into an internal time-series.
     * @param dataSource the data source
     * @param timeSeries the time-series to transform
     * @param links the links
     * @return the internal time-series
     */
    private TimeSeriesTuple transform( DataSource dataSource,
                                       List<wres.reading.nwis.ogc.response.Feature> timeSeries,
                                       Link[] links )
    {
        // Get the link to the next page of data
        URI next = Arrays.stream( links )
                         .filter( l -> l.getRel()
                                        .equals( "next" ) )
                         .map( Link::getHref )
                         .findFirst()
                         .orElse( null );  // Clear the next page when all pages are exhausted

        // Get the adjusted data source with the next page of data
        DataSource adjustedDataSource = dataSource.toBuilder()
                                                  .nextPage( next )
                                                  .build();

        // Get the metadata
        TimeSeriesMetadata metadata = this.getTimeSeriesMetadata( timeSeries.get( 0 ),
                                                                  dataSource );
        SortedSet<Event<Double>> events = this.getTimeSeriesEvents( timeSeries );

        TimeSeries<Double> series = TimeSeries.of( metadata, events );

        return TimeSeriesTuple.ofSingleValued( series, adjustedDataSource );
    }

    /**
     * Return the time-series events
     * @param timeSeries the raw time-series events
     * @return the composed time-series events
     */

    private SortedSet<Event<Double>> getTimeSeriesEvents( List<Feature> timeSeries )
    {
        SortedSet<Event<Double>> events = new TreeSet<>();
        for ( Feature nextValue : timeSeries )
        {
            Properties properties = nextValue.getProperties();
            double value = properties.getValue();

            String time = properties.getTime();
            Instant instant = this.getTimeInstant( time, properties.getLocationId() );

            Event<Double> nextEvent = DoubleEvent.of( instant, value );
            events.add( nextEvent );
        }

        return events;
    }

    /**
     * Parses a time, which is either a local time or an offset time.
     *
     * @param time the raw datetime
     * @param locationId the location identifier
     * @return the instant
     */

    private Instant getTimeInstant( String time,
                                    String locationId )
    {
        Instant instant;

        // Offset time or local time?
        if ( time.contains( "+" ) )
        {
            OffsetDateTime offsetTime = OffsetDateTime.parse( time );
            instant = offsetTime.toInstant();
        }
        else
        {
            LocationMetadata metadata = this.getLocationMetadata( locationId );

            LocalDate date = LocalDate.parse( time );

            // Values are assumed to be midnight in local time, which is true for the DV service, for example
            LocalDateTime localTime = LocalDateTime.of( date, LocalTime.MIDNIGHT );

            // Full time zone information supplied
            if ( Objects.nonNull( metadata.timeZone() ) )
            {
                instant = localTime.atZone( metadata.timeZone()
                                                    .toZoneId() )
                                   .toInstant();
            }
            // Partial information supplied, a zone offset only
            else
            {
                instant = localTime.toInstant( metadata.zoneOffset() );
            }
        }

        return instant;
    }

    /**
     * Attempts to acquire the location metadata for a given location identifier.
     *
     * @param locationId the location identifier
     * @return the location metadata
     * @throws ReadException if the location could not be found
     */


    private LocationMetadata getLocationMetadata( String locationId )
    {
        LocationMetadata metadata = this.locationMetadata.getIfPresent( locationId );

        if ( Objects.isNull( metadata ) )
        {
            throw new ReadException( "Could not convert a local datetime to UTC for a time-series associated with "
                                     + "location, " + locationId + ", because the location metadata for "
                                     + locationId + " was not supplied to the reader. Metadata was available for "
                                     + "the following locations: " + this.locationMetadata.asMap()
                                                                                          .keySet() );
        }

        return metadata;
    }

    /**
     * Generates a {@link TimeSeriesMetadata} from the input.
     * @param feature the feature source
     * @param dataSource the data source
     * @return the time-series metadata
     */

    private TimeSeriesMetadata getTimeSeriesMetadata( wres.reading.nwis.ogc.response.Feature feature,
                                                      DataSource dataSource )
    {
        Properties properties = feature.getProperties();
        String locationId = properties.getLocationId();

        // Add the location metadata, if available
        LocationMetadata metadata = this.locationMetadata.getIfPresent( locationId );

        // Format promise is a feature authority or agency code followed by a feature identifier. Remove the authority.
        if ( locationId.contains( "-" ) )
        {
            locationId = locationId.substring( locationId.lastIndexOf( "-" ) + 1 );
        }

        String wkt = "";
        String description = "";

        // Fixed to EPSG:4326, see docs: https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/schema
        int srid = 4326;

        // Supplied in a cache based on a central request
        if ( Objects.nonNull( metadata )
             && Objects.nonNull( metadata.geometry() ) )
        {
            org.locationtech.jts.geom.Geometry geometry = metadata.geometry();
            wkt = geometry.toText();
            if( Objects.nonNull( metadata.description() ) )
            {
                description = metadata.description();
            }
        }
        // Supplied within the time-series response itself
        else if ( Objects.nonNull( feature.getGeometry() ) )
        {
            wkt = feature.getGeometry()
                         .toText();
        }

        Geometry geometry = Geometry.newBuilder()
                                    .setName( locationId )
                                    .setWkt( wkt )
                                    .setSrid( srid )
                                    .setDescription( description )
                                    .build();
        wres.datamodel.space.Feature internalFeature = wres.datamodel.space.Feature.of( geometry );

        String statistic = properties.getStatistic();
        TimeScaleOuter timeScale = this.getTimeScale( statistic );
        TimeSeriesMetadata.Builder builder = new TimeSeriesMetadata.Builder();
        return builder.setFeature( internalFeature )
                      // Obtain from data source rather than properties as properties are streamlined
                      .setVariableName( dataSource.getVariable()
                                                  .name() )
                      .setUnit( properties.getUnit() )
                      .setReferenceTimes( Map.of() ) // No reference time
                      .setTimeScale( timeScale )
                      .build();
    }

    /**
     * Generates the timescale metadata from the statistic string.
     * @param statistic the statistic
     * @return the timescale
     * @throws NullPointerException if the input is null
     */

    private TimeScaleOuter getTimeScale( String statistic )
    {
        Objects.requireNonNull( statistic );

        if ( "00011".equals( statistic ) )
        {
            LOGGER.debug( "Detected an instantaneous time-scale with statistic identifier, '00011'." );

            return TimeScaleOuter.of();
        }

        // There are currently two OGC services, each of which is associated with time-series that that has a fixed
        // timescale. Since this is not instantaneous data (above), it must be daily data. If other services are added,
        // this logic will need to become more sophisticated
        LOGGER.debug( "Assuming that the time-series data has a time-scale of one day." );

        TimeScale.TimeScaleFunction scaleFunction = this.getTimeScaleFunction( statistic );
        Duration period = MessageUtilities.getDuration( java.time.Duration.ofDays( 1 ) );
        TimeScale timeScale = TimeScale.newBuilder().setFunction( scaleFunction )
                                       .setPeriod( period )
                                       .build();
        return TimeScaleOuter.of( timeScale );
    }

    /**
     * Translates the supplied statistic string into an established timescale function or
     * {@link TimeScale.TimeScaleFunction#UNKNOWN}.
     */

    private TimeScale.TimeScaleFunction getTimeScaleFunction( String statistic )
    {
        return switch ( statistic )
        {
            case "00001" -> TimeScale.TimeScaleFunction.MAXIMUM;
            case "00002" -> TimeScale.TimeScaleFunction.MINIMUM;
            case "00003" -> TimeScale.TimeScaleFunction.MEAN;
            case "00006" -> TimeScale.TimeScaleFunction.TOTAL;
            default ->
            {
                LOGGER.trace( "Encountered an unknown time-scale function when reading time-series values: {}",
                              statistic );
                yield TimeScale.TimeScaleFunction.UNKNOWN;
            }
        };
    }

    /**
     * Hidden constructor.
     * @param locationMetadata the cache of location metadata
     */

    private NwisResponseReader( Cache<@NonNull String, LocationMetadata> locationMetadata )
    {
        Objects.requireNonNull( locationMetadata );
        this.locationMetadata = locationMetadata;
    }
}
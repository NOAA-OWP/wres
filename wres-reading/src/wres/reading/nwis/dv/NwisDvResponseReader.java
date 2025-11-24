package wres.reading.nwis.dv;

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
import java.time.ZoneOffset;
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.protobuf.Duration;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import wres.reading.nwis.dv.response.Feature;
import wres.reading.nwis.dv.response.Link;
import wres.reading.nwis.dv.response.Properties;
import wres.reading.nwis.dv.response.Response;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale;

/**
 * <p>Reads time-series data responses from the National Water Information System Daily Values web service.
 *
 * <p>Implementation notes:
 *
 * <p>This implementation simply reads the responses from a file source or supplied stream and does not interact with
 * the web service that originally supplied the responses. In other words, any contextual information that is required
 * at read time should be supplied on construction. This includes any information that is obtained from a separate
 * endpoint of the web API, which could be provided as a deferred function call (i.e., {@link Supplier}), for example.
 * Currently, this reader is not embellished with any external information supplied on construction, but does require
 * the {@link DataSource} that is supplied at read time to contain a {@link ZoneOffset}, which may be obtained
 * externally or declared by a user.
 *
 * @author James Brown
 */
public class NwisDvResponseReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwisDvResponseReader.class );

    /** Maps WaterML bytes to POJOs. */
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    /**
     * Creates an instance.
     *
     * @return an instance
     */

    public static NwisDvResponseReader of()
    {
        return new NwisDvResponseReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource, false );

        try
        {
            Path path = Paths.get( dataSource.getUri() );
            InputStream stream = Files.newInputStream( path );
            return this.readFromStream( dataSource, stream );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a WaterML source.", e );
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
        Objects.requireNonNull( inputStream );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataSource.DataDisposition.GEOJSON );

        // Validate the time zone offset, which is required to read from the NWIS DV service
        this.validateTimeZoneOffsetPresent( dataSource );

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
                                          dataSource.getUri() );
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
                              dataSource.getUri(),
                              new String( rawForecast,
                                          StandardCharsets.UTF_8 ) );
            }

            Response response = OBJECT_MAPPER.readValue( rawForecast, Response.class );

            List<TimeSeriesTuple> allTimeSeries = new ArrayList<>();

            // Some time-series with data
            if ( response.getNumberReturned() > 0 )
            {
                Map<String, List<wres.reading.nwis.dv.response.Feature>> bySeries =
                        Arrays.stream( response.getFeatures() )
                              .collect( Collectors.groupingBy( f -> f.getProperties()
                                                                     .getTimeSeriesId() ) );
                for ( List<wres.reading.nwis.dv.response.Feature> nextValues : bySeries.values() )
                {
                    TimeSeriesTuple nextSeries = this.transform( dataSource, nextValues, response.getLinks() );
                    allTimeSeries.add( nextSeries );
                }
            }

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Read {} time series from {}.",
                              allTimeSeries.size(),
                              dataSource.getUri() );
            }

            return Collections.unmodifiableList( allTimeSeries );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read the WaterML data stream.", e );
        }
    }

    /**
     * Transforms a WaterML time-series into an internal time-series.
     * @param dataSource the data source
     * @param timeSeries the time-series to transform
     * @param links the links
     * @return the internal time-series
     */
    private TimeSeriesTuple transform( DataSource dataSource,
                                       List<wres.reading.nwis.dv.response.Feature> timeSeries,
                                       Link[] links )
    {
        // Get the link to the next page of data
        URI next = Arrays.stream( links ).filter( l -> l.getRel()
                                                        .equals( "next" ) )
                         .map( Link::getHref )
                         .findFirst()
                         .orElse( null );

        // Get the adjusted data source with the next page of data
        DataSource adjustedDataSource = dataSource.toBuilder()
                                                  .nextPage( next )
                                                  .build();

        // Get the metadata
        TimeSeriesMetadata metadata = this.getTimeSeriesMetadata( timeSeries.get( 0 ) );
        SortedSet<Event<Double>> events = this.getTimeSeriesEvents( timeSeries, dataSource.getSource()
                                                                                          .timeZoneOffset() );

        TimeSeries<Double> series = TimeSeries.of( metadata, events );

        return TimeSeriesTuple.ofSingleValued( series, adjustedDataSource );
    }

    /**
     * Return the time-series events
     * @param timeSeries the raw time-series events
     * @return the composed time-series events
     */

    private SortedSet<Event<Double>> getTimeSeriesEvents( List<Feature> timeSeries,
                                                          ZoneOffset zoneOffset )
    {
        SortedSet<Event<Double>> events = new TreeSet<>();
        for ( Feature nextValue : timeSeries )
        {
            Properties properties = nextValue.getProperties();
            double value = properties.getValue();
            LocalDate date = LocalDate.parse( properties.getTime() );

            // Values are midnight in local time
            LocalDateTime time = LocalDateTime.of( date, LocalTime.MIDNIGHT );
            Instant instant = time.toInstant( zoneOffset );
            Event<Double> nextEvent = DoubleEvent.of( instant, value );
            events.add( nextEvent );
        }

        return events;
    }

    /**
     * Generates a {@link TimeSeriesMetadata} from the input.
     * @param feature the feature source
     * @return the time-series metadata
     */

    private TimeSeriesMetadata getTimeSeriesMetadata( wres.reading.nwis.dv.response.Feature feature )
    {
        String wkt = feature.getGeometry()
                            .toText();
        Properties properties = feature.getProperties();
        String locationId = properties.getLocationId();
        // Format promise is a feature authority or agency code followed by a feature identifier. Remove the authority.
        if ( locationId.contains( "-" ) )
        {
            locationId = locationId.substring( locationId.lastIndexOf( "-" ) + 1 );
        }

        Geometry geometry = Geometry.newBuilder()
                                    .setName( locationId )
                                    .setWkt( wkt )
                                    .setSrid( feature.getGeometry()
                                                     .getSRID() )
                                    .build();
        wres.datamodel.space.Feature internalFeature = wres.datamodel.space.Feature.of( geometry );

        String statistic = properties.getStatistic();
        TimeScale.TimeScaleFunction scaleFunction = this.getTimeScaleFunction( statistic );
        Duration period = MessageUtilities.getDuration( java.time.Duration.ofDays( 1 ) );
        TimeScale timeScale = TimeScale.newBuilder().setFunction( scaleFunction )
                                       .setPeriod( period )
                                       .build();
        TimeScaleOuter timeScaleOuter = TimeScaleOuter.of( timeScale );
        TimeSeriesMetadata.Builder builder = new TimeSeriesMetadata.Builder();
        return builder.setFeature( internalFeature )
                      .setVariableName( properties.getParameterCode() )
                      .setUnit( properties.getUnit() )
                      .setReferenceTimes( Map.of() ) // No reference time
                      .setTimeScale( timeScaleOuter )
                      .build();
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
                LOGGER.trace( "Encountered an unknown time-scale function when reading NWIS daily values: {}",
                              statistic );
                yield TimeScale.TimeScaleFunction.UNKNOWN;
            }
        };
    }

    /**
     * Checks that the time zone offset is present within the data source.
     * @param dataSource the data source
     */

    private void validateTimeZoneOffsetPresent( DataSource dataSource )
    {
        if ( Objects.isNull( dataSource.getSource()
                                       .timeZoneOffset() ) )
        {
            throw new ReadException( "The time zone offset must be declared when reading time-series data from the "
                                     + "National Water Information System Daily Values web service. Please declare the "
                                     + "time zone offset and try again." );
        }
    }

    /**
     * Hidden constructor.
     */

    private NwisDvResponseReader()
    {
    }
}
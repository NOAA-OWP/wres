package wres.io.reading.wrds;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import wres.config.generated.DatasourceType;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesTuple;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.TimeSeriesReader;
import wres.statistics.generated.Geometry;

/**
 * <p>Reads time-series data from the U.S. National Weather Service (NWS) Advanced Hydrologic Prediction Service (AHPS) 
 * supplied in a JSON time-series format defined by the NWS Water Resources Data Service (WRDS).
 * 
 * <p>Implementation notes:
 * 
 * <p>This reader currently performs eager reading of time-series data. It relies on the Jackson framework, 
 * specifically an {@link ObjectMapper}, which maps a JSON byte array into time-series objects. An improved 
 * implementation would stream the underlying bytes into {@link TimeSeries} on demand. Thus, particularly when the 
 * underlying data source is a large file or a large stream that is not chunked at a higher level, this implementation
 * is not very memory efficient, contrary to the recommended implementation in {@link TimeSeriesReader}.
 * 
 * TODO: consider a more memory efficient implementation by using the Jackson streaming API. For example: 
 * https://www.baeldung.com/jackson-streaming-api. As of v6.7, this is not a tremendous problem because the main
 * application of this class is reading directly from WRDS whereby the responses are chunked at a higher level. However, 
 * this limitation would become more acute were there a need to read a large WRDS-formatted JSON file from a local disk.
 * 
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class WrdsAhpsJsonReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsAhpsJsonReader.class );

    /** Maps JSON bytes to POJOs. */
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    /**
     * @return an instance
     */

    public static WrdsAhpsJsonReader of()
    {
        return new WrdsAhpsJsonReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

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
     * Reads WaterML data from a stream.
     * @param dataSource the data source
     * @param inputStream the data stream
     * @return the time-series streams
     * @throws NullPointerException if either input is null
     */
    private Stream<TimeSeriesTuple> readFromStream( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        // Get the lazy supplier of time-series data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, inputStream );

        // Generate a stream of time-series.
        // This is merely a facade on incremental reading until the underlying supplier reads incrementally
        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     // Close the data provider when the stream is closed
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event, closing an underlying data provider." );

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
     * Returns a time-series supplier from the inputs. Currently, this method performs eager reading.
     * 
     * TODO: implement incremental reading using the Jackson Streaming API or similar.
     * 
     * @param dataSource the data source
     * @param inputStream the stream to read
     * @return a time-series supplier
     * @throws ReadException if the data could not be read for any reason
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             InputStream inputStream )
    {
        // Read the time-series eagerly
        List<TimeSeriesTuple> timeSeriesTuples = this.getTimeSeries( dataSource, inputStream );
        AtomicInteger iterator = new AtomicInteger();

        // Create a supplier that returns a time-series once complete
        return () -> {

            // More time-series to return?
            if ( iterator.get() < timeSeriesTuples.size() )
            {
                return timeSeriesTuples.get( iterator.getAndIncrement() );
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
        URI uri = dataSource.getUri();

        try
        {
            byte[] rawForecast = inputStream.readAllBytes();

            if ( Objects.isNull( rawForecast ) )
            {
                return Collections.emptyList();
            }

            // It is conceivable that we could tee/pipe the data to both
            // the md5sum and the parser at the same time, but this involves
            // more complexity and may not be worth it. For now assume that we are
            // not going to exhaust our heap by including the whole forecast
            // here in memory temporarily.

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Time series as bytes: {} and as UTF-8: {}",
                              rawForecast,
                              new String( rawForecast,
                                          StandardCharsets.UTF_8 ) );
            }

            ForecastResponse response = OBJECT_MAPPER.readValue( rawForecast,
                                                                 ForecastResponse.class );

            Forecast[] forecasts = response.getForecasts();

            if ( Objects.isNull( forecasts ) )
            {
                throw new ReadException( "Failed to obtain a response from the WRDS url "
                                         + uri
                                         + "Was the correct URL provided in the declaration?" );
            }

            // The response should include the missing values, but, in case we reuse
            // this code later to read other forecasts, I allow for null.  If not null
            // the output the list of missing values to debug.
            double[] missingValues = response.getHeader()
                                             .getMissing_values();

            if ( LOGGER.isDebugEnabled() )
            {
                if ( Objects.nonNull( missingValues ) )
                {
                    LOGGER.debug( "The time series specified the following missing values: {}.",
                                  Arrays.toString( response.getHeader()
                                                           .getMissing_values() ) );
                }
                else
                {
                    LOGGER.debug( "The time series specified no missing values." );
                }
            }

            List<TimeSeriesTuple> results = new ArrayList<>( forecasts.length );

            for ( Forecast forecast : forecasts )
            {
                LOGGER.debug( "Parsing {}", forecast );
                TimeSeriesTuple timeSeriesTuple = this.getTimeSeries( forecast,
                                                                      missingValues,
                                                                      dataSource );

                results.add( timeSeriesTuple );
            }

            return Collections.unmodifiableList( results );
        }
        catch ( IOException je )
        {
            throw new ReadException( "Failed to parse the response body from WRDS url "
                                     + uri,
                                     je );
        }
    }

    /**
     * 
     * @param forecast the forecast
     * @param missingValues the missing values, optional
     * @param dataSource the data source
     * @return the internal time-series
     */

    private TimeSeriesTuple getTimeSeries( Forecast forecast,
                                           double[] missingValues,
                                           DataSource dataSource )
    {
        List<DataPoint> dataPointsList;
        URI uri = dataSource.getUri();
        Member[] members = forecast.getMembers();
        if ( Objects.nonNull( members )
             && members.length > 0
             && !members[0].getDataPointsList()
                           .isEmpty() )
        {
            dataPointsList = members[0].getDataPointsList()
                                       .get( 0 );
        }
        else
        {
            LOGGER.warn( "No time-series events were discovered in the time series '{}' at '{}'.",
                         forecast,
                         uri );

            return null;
        }

        Map<ReferenceTimeType, Instant> datetimes = new EnumMap<>( ReferenceTimeType.class );

        if ( Objects.nonNull( forecast.getBasisTime() ) )
        {
            Instant basisDateTime = forecast.getBasisTime()
                                            .toInstant();
            datetimes.put( ReferenceTimeType.T0, basisDateTime );
        }

        if ( Objects.nonNull( forecast.getIssuedTime() ) )
        {
            Instant issuedDateTime = forecast.getIssuedTime()
                                             .toInstant();
            datetimes.put( ReferenceTimeType.ISSUED_TIME, issuedDateTime );
        }

        // If datetimes is empty, then, if the data are observations, use the latest time
        // associated with an observation as a "dummy" reference time.  Otherwise, the 
        // the data is either a forecast or simulation, and a reference this is required.
        // Since its not found, skip the time series with an appropriate message.
        if ( datetimes.isEmpty() && dataSource.getContext()
                                              .getType() != DatasourceType.OBSERVATIONS )
        {
            LOGGER.warn( "Forecast at {} did not have a reference datetime. Skipping it.",
                         uri );
            return null;

        }

        // Get the time scale information, if available
        TimeScaleOuter timeScale = TimeScaleFromParameterCodes.getTimeScale( forecast.getParameterCodes(), uri );
        String measurementUnit = members[0].getUnits();
        if ( Objects.isNull( measurementUnit ) && Objects.nonNull( forecast.getUnits() ) )
        {
            measurementUnit = forecast.getUnits()
                                      .getUnitName();
        }

        String variableName = forecast.getParameterCodes()
                                      .getPhysicalElement();

        String featureName = forecast.getLocation()
                                     .getNames()
                                     .getNwsLid();

        String featureDescription = forecast.getLocation()
                                            .getNames()
                                            .getNwsName();

        Geometry geometry = MessageFactory.getGeometry( featureName, featureDescription, null, null );
        FeatureKey feature = FeatureKey.of( geometry );

        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( datetimes,
                                                             timeScale,
                                                             variableName,
                                                             feature,
                                                             measurementUnit );

        // Before ingest, validate the timeseries as being a timeseries in the
        // sense that a timeseries is a sequence of values in time.
        this.validateTimeseries( dataPointsList, uri );

        TimeSeries.Builder<Double> timeSeriesBuilder =
                new TimeSeries.Builder<Double>().setMetadata( metadata );

        for ( DataPoint dataPoint : dataPointsList )
        {
            double usedValue = dataPoint.getValue();

            // If the read value matches a missing, replace it with the internal missing
            if ( Objects.nonNull( missingValues )
                 && Arrays.stream( missingValues )
                          .anyMatch( missingValue -> Precision.equals( dataPoint.getValue(),
                                                                       missingValue,
                                                                       8 ) ) )
            {
                usedValue = MissingValues.DOUBLE;
            }

            Event<Double> event = Event.of( dataPoint.getTime()
                                                     .toInstant(),
                                            usedValue );

            timeSeriesBuilder.addEvent( event );
        }

        TimeSeries<Double> timeSeries = timeSeriesBuilder.build();

        return TimeSeriesTuple.ofSingleValued( timeSeries );
    }

    /**
     * Validate a timeseries. A timeseries according to this method is a sequence of values in time. Therefore, a list 
     * of data with duplicate values for any given datetime is invalid.
     *
     * @param dataPointsList the WRDS-formatted timeseries data points
     * @param uri the data source uri
     * @throws ReadException when invalid timeseries found
     */

    private void validateTimeseries( List<DataPoint> dataPointsList, URI uri )
    {
        Objects.requireNonNull( dataPointsList );

        // Put each datetime in a set. We can compare the set size to the list
        // size and if they are identical: all good.
        Set<OffsetDateTime> dateTimes = new HashSet<>( dataPointsList.size() );

        // For error message purposes, track the exact datetimes that had more
        // than one value.
        Set<OffsetDateTime> multipleValues = new TreeSet<>();

        for ( DataPoint wrdsDataPoint : dataPointsList )
        {
            OffsetDateTime dateTimeForOneValue = wrdsDataPoint.getTime();
            boolean added = dateTimes.add( dateTimeForOneValue );

            if ( !added )
            {
                multipleValues.add( dateTimeForOneValue );
            }
        }

        // Check the size of the datetimes set vs the size of the list
        if ( dataPointsList.size() != dateTimes.size() )
        {
            String message = "Invalid time series data encountered. Multiple data"
                             + " found for each of the following datetimes in "
                             + "a time series from "
                             + uri
                             + " : "
                             + multipleValues;
            throw new ReadException( message );
        }
    }

    /**
     * Hidden constructor.
     */

    private WrdsAhpsJsonReader()
    {
    }
}

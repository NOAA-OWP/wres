package wres.io.reading.waterml;

import static org.apache.commons.math3.util.Precision.EPSILON;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.waterml.timeseries.GeographicLocation;
import wres.io.reading.waterml.timeseries.Method;
import wres.io.reading.waterml.timeseries.SiteCode;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Reads time-series data formatted in WaterML. Further information on the Water ML standard can be found here:
 * 
 * <p><a href="https://www.ogc.org/standards/waterml">WaterML</a> 
 * 
 * <p>The above link was last accessed: 20220803T12:00Z.
 * 
 * <p>Implementation notes:
 * 
 * <p>This reader currently performs eager reading of time-series data. It relies on the Jackson framework, 
 * specifically an {@link ObjectMapper}, which maps a WaterML byte array into time-series objects. An improved 
 * implementation would stream the underlying bytes into {@link TimeSeries} on demand. Thus, particularly when the 
 * underlying data source is a large file or a large stream that is not chunked at a higher level, this implementation
 * is not very memory efficient, contrary to the recommended implementation in {@link TimeSeriesReader}.
 * 
 * <p>This implementation currently assumes time-series with a fixed time-scale. This it obtained from the URI embedded
 * within the data source via {@link ReaderUtilities#getTimeScaleFromUri(java.net.URI)}.
 * 
 * TODO: consider a more memory efficient implementation by using the Jackson streaming API. For example: 
 * https://www.baeldung.com/jackson-streaming-api. As of v6.7, this is not a tremendous problem because the main
 * application of this class is reading from USGS NWIS whereby the responses are chunked. However, this limitation 
 * would become more acute were there a need to read a large waterml file from a local disk.
 * 
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class WatermlReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WatermlReader.class );

    /** Maps WaterML bytes to POJOs. */
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    /**
     * @return an instance
     */

    public static WatermlReader of()
    {
        return new WatermlReader();
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

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.JSON_WATERML );
        
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
        AtomicInteger iterator = new AtomicInteger();
        AtomicReference<List<TimeSeriesTuple>> timeSeriesTuples = new AtomicReference<>();

        // Create a supplier that returns the time-series
        return () -> {

            // Read all of the time-series eagerly on first use: this will still delay any read until a terminal stream
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
            if ( response.getValue()
                         .getNumberOfPopulatedTimeSeries() > 0 )
            {
                for ( wres.io.reading.waterml.timeseries.TimeSeries series : response.getValue().getTimeSeries() )
                {
                    List<TimeSeriesTuple> nextSeries = this.transform( dataSource, series );
                    allTimeSeries.addAll( nextSeries );
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
     * @return the internal time-series
     */
    private List<TimeSeriesTuple> transform( DataSource dataSource,
                                             wres.io.reading.waterml.timeseries.TimeSeries timeSeries )
    {
        // Nothing?
        if ( !timeSeries.isPopulated() )
        {
            LOGGER.debug( "Discovered an empty Water ML time-series." );
            return Collections.emptyList();
        }

        // Get the first variable name from the series in the actual data.
        if ( timeSeries.getVariable() == null
             || timeSeries.getVariable().getVariableCode() == null
             || timeSeries.getVariable().getVariableCode().length < 1
             || timeSeries.getVariable().getVariableCode()[0].getValue() == null )
        {
            LOGGER.debug( "No variable found for timeseries {} in source {}.",
                          timeSeries,
                          dataSource.getUri() );

            return Collections.emptyList();
        }

        String variableName = timeSeries.getVariable()
                                        .getVariableCode()[0]
                                                             .getValue();

        // Get the measurement unit from the series in the actual data.
        // (Assumes above check for variable has already happened as well)
        if ( timeSeries.getVariable().getUnit() == null
             || timeSeries.getVariable().getUnit().getUnitCode() == null )
        {
            LOGGER.warn( "No unit found for timeseries {} in source {}.",
                         timeSeries,
                         dataSource.getUri() );

            return Collections.emptyList();
        }

        String unitCode = timeSeries.getVariable()
                                    .getUnit()
                                    .getUnitCode();

        if ( timeSeries.getSourceInfo() == null
             || timeSeries.getSourceInfo().getSiteCode() == null
             || timeSeries.getSourceInfo().getSiteCode().length < 1 )
        {
            LOGGER.debug( "No site code found for timeseries {} in source {}.",
                          timeSeries,
                          dataSource.getUri() );
            return Collections.emptyList();
        }

        List<String> siteCodesFound = Arrays.stream( timeSeries.getSourceInfo().getSiteCode() )
                                            .map( SiteCode::getValue )
                                            .filter( Objects::nonNull )
                                            .collect( Collectors.toList() );

        if ( siteCodesFound.size() != 1 )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Expected exactly one site code, but found {} for timeseries {} in source {}.",
                              siteCodesFound.size(),
                              timeSeries,
                              dataSource.getUri() );
            }

            return Collections.emptyList();
        }

        String featureName = siteCodesFound.get( 0 );
        FeatureKey featureKey = this.translateGeographicFeature( dataSource,
                                                                 featureName,
                                                                 timeSeries );

        // Attempt to guess the time-scale from the URI
        TimeScaleOuter timeScale = ReaderUtilities.getTimeScaleFromUri( dataSource.getUri() );

        // #104572
        int countOfTracesFound = this.getCountOfTracesWithData( timeSeries.getValues() );

        if ( countOfTracesFound > 1 )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "Skipping site {} because multiple timeseries for variable {} from URI {}.",
                             featureName,
                             variableName,
                             dataSource.getUri() );
            }

            return Collections.emptyList();
        }

        return this.transform( dataSource, timeSeries, featureKey, variableName, unitCode, timeScale );
    }

    /**
     * Transforms a WaterML time-series into an internal time-series.
     * @param dataSource the data source
     * @param timeSeries the time-series to transform
     * @return the internal time-series
     */

    private List<TimeSeriesTuple> transform( DataSource dataSource,
                                             wres.io.reading.waterml.timeseries.TimeSeries timeSeries,
                                             FeatureKey featureKey,
                                             String variableName,
                                             String unitCode,
                                             TimeScaleOuter timeScale )
    {
        List<TimeSeriesTuple> timeSeriesTuples = new ArrayList<>();

        for ( TimeSeriesValues valueSet : timeSeries.getValues() )
        {
            if ( valueSet.getValue().length == 0 )
            {
                continue;
            }

            Method[] methods = valueSet.getMethod();

            if ( Objects.nonNull( methods ) && methods.length > 0 )
            {
                for ( Method method : methods )
                {
                    LOGGER.debug( "Found method id={} description='{}'.",
                                  method.getMethodID(),
                                  method.getMethodDescription() );
                }
            }

            LOGGER.trace( "Jackson parsed these for site {} variable {}: {}.",
                          featureKey,
                          variableName,
                          valueSet );

            Double noDataValue = timeSeries.getVariable()
                                           .getNoDataValue();

            SortedSet<Event<Double>> rawTimeSeries = this.readTimeSeriesValues( valueSet, noDataValue );

            Map<ReferenceTimeType, Instant> referenceTimes = Map.of();
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( referenceTimes,
                                                                 timeScale,
                                                                 variableName,
                                                                 featureKey,
                                                                 unitCode );

            LOGGER.trace( "TimeSeries parsed for {}: {}.", metadata, rawTimeSeries );

            try
            {
                TimeSeries<Double> timeSeriesInternal = TimeSeries.of( metadata, rawTimeSeries );
                
                // Validate
                ReaderUtilities.validateAgainstEmptyTimeSeries( timeSeriesInternal, dataSource.getUri() );
                
                TimeSeriesTuple tuple = TimeSeriesTuple.ofSingleValued( timeSeriesInternal, dataSource );                
                timeSeriesTuples.add( tuple );
            }
            catch ( IllegalArgumentException iae )
            {
                throw new ReadException( "While creating timeseries for site "
                                         + featureKey
                                         + " from "
                                         + dataSource.getUri()
                                         + ": ",
                                         iae );
            }
        }

        return Collections.unmodifiableList( timeSeriesTuples );
    }

    /**
     * Reads the time-series events from the input.
     * @param values the event values
     * @param noDataValue the missing value
     * @return the time-series events
     */
    private SortedSet<Event<Double>> readTimeSeriesValues( TimeSeriesValues values, Double noDataValue )
    {
        SortedSet<Event<Double>> rawTimeSeries = new TreeSet<>();
        for ( TimeSeriesValue value : values.getValue() )
        {
            double readValue = value.getValue();

            if ( Objects.nonNull( noDataValue )
                 && Precision.equals( readValue, noDataValue, EPSILON ) )
            {
                readValue = MissingValues.DOUBLE;
            }

            Instant dateTime = value.getDateTime();
            Event<Double> event = Event.of( dateTime, readValue );
            rawTimeSeries.add( event );
        }

        return Collections.unmodifiableSortedSet( rawTimeSeries );
    }

    /**
     * @param traces the traces
     * @return the count of traces that contain one or more valid times and values.
     */

    private int getCountOfTracesWithData( TimeSeriesValues[] timeSeriesValues )
    {
        int count = 0;
        for ( TimeSeriesValues nextValues : timeSeriesValues )
        {
            if ( nextValues.getValue().length > 0 )
            {
                count++;
            }

        }
        return count;
    }

    /**
     * Translate the a geographic feature into WRES format.
     * @param dataSource the data source
     * @param featureName the feature name already found and validated
     * @param series the series to get geographic feature data from
     * @return a WRES feature
     */

    private FeatureKey translateGeographicFeature( DataSource dataSource,
                                                   String featureName,
                                                   wres.io.reading.waterml.timeseries.TimeSeries series )
    {
        String siteDescription = null;
        Integer siteSrid = null;
        String siteWkt = null;
        GeographicLocation geographicLocation = null;

        if ( Objects.nonNull( series.getSourceInfo()
                                    .getGeoLocation() ) )
        {
            siteDescription = series.getSourceInfo()
                                    .getSiteName();

            geographicLocation = series.getSourceInfo()
                                       .getGeoLocation()
                                       .getGeogLocation();
        }

        if ( Objects.nonNull( geographicLocation ) )
        {
            if ( Objects.nonNull( geographicLocation.getSrs() ) )
            {
                String rawSrs = geographicLocation.getSrs()
                                                  .strip();

                if ( rawSrs.startsWith( "EPSG:" ) )
                {
                    String srid = rawSrs.substring( 5 );

                    try
                    {
                        siteSrid = Integer.valueOf( srid );
                    }
                    catch ( NumberFormatException nfe )
                    {
                        LOGGER.warn( "Unable to extract SRID from SRS {} in timeseries for site code {} at url {}",
                                     rawSrs,
                                     featureName,
                                     dataSource.getUri() );
                    }
                }
            }

            if ( Objects.nonNull( geographicLocation.getLatitude() )
                 && Objects.nonNull( geographicLocation.getLongitude() ) )
            {
                StringJoiner point = new StringJoiner( " " );
                point.add( "POINT (" );

                // WKT is x,y aka lon,lat
                point.add( geographicLocation.getLongitude()
                                             .toString() );
                point.add( geographicLocation.getLatitude()
                                             .toString() );
                point.add( ")" );
                siteWkt = point.toString();
            }
        }

        Geometry geometry = MessageFactory.getGeometry( featureName, siteDescription, siteSrid, siteWkt );
        return FeatureKey.of( geometry );
    }

    /**
     * Hidden constructor.
     */

    private WatermlReader()
    {
    }

}

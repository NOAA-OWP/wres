package wres.reading.wrds.nwm;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.reading.DataSource.DataDisposition;
import wres.reading.wrds.ahps.TimeScaleFromParameterCodes;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Reads time-series data from the U.S. National Weather Service (NWS) National Water Model (NWM) supplied in a JSON 
 * time-series format defined by the NWS Water Resources Data Service (WRDS).
 *
 * <p>Implementation notes:
 *
 * <p>This reader currently performs eager reading of time-series data. It relies on the Jackson framework, 
 * specifically an {@link ObjectMapper}, which maps a JSON byte array into time-series objects. An improved 
 * implementation would stream the underlying bytes into {@link TimeSeries} on demand. Thus, particularly when the 
 * underlying data source is a large file or a large stream that is not chunked at a higher level, this implementation
 * is not very memory efficient, contrary to the recommended implementation in {@link TimeSeriesReader}.
 *
 * <p>TODO: consider a more memory efficient implementation by using the Jackson streaming API. For example:
 * <a href="https://www.baeldung.com/jackson-streaming-api">Jackson</a>. As of v6.7, this is not a tremendous problem
 * because the main application of this class is reading directly from WRDS whereby the responses are chunked at a
 * higher level. However, this limitation would become more acute were there a need to read a large WRDS-formatted
 * JSON file from a local disk.
 *
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class WrdsNwmJsonReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmJsonReader.class );

    /** Maps JSON bytes to POJOs. */
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

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
            Path path = Paths.get( dataSource.getUri() );
            InputStream stream = new BufferedInputStream( Files.newInputStream( path ) );
            return this.read( dataSource, stream );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a WaterML source.", e );
        }
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.JSON_WRDS_NWM );

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
     * <p>Returns a time-series supplier from the inputs. Currently, this method performs eager reading.
     *
     * <p>TODO: implement incremental reading using the Jackson Streaming API or similar.
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

        // Create a supplier that returns a time-series once complete
        return () -> {

            // Read all of the time-series eagerly on first use: this will still delay any read until a terminal stream
            // operation pulls from the supplier (which is why we use a reference holder and do not request the 
            // time-series outside of this lambda), but it will then acquire all the time-series eagerly, i.e., now
            if ( Objects.isNull( timeSeriesTuples.get() ) )
            {
                List<TimeSeriesTuple> eagerSeries = this.getTimeSeries( dataSource, inputStream );

                LOGGER.debug( "Read {} time-series from {}.", eagerSeries.size(), dataSource );

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
        URI uri = dataSource.getUri();
        NwmRootDocument rootDocument = this.getNwmRootDocument( inputStream, uri );

        // Validate
        if ( Objects.isNull( rootDocument ) )
        {
            LOGGER.debug( "Failed to read a root document from {}.", uri );
            return Collections.emptyList();
        }

        List<String> wrdsWarnings = rootDocument.getWarnings();

        if ( Objects.nonNull( wrdsWarnings ) && !wrdsWarnings.isEmpty() )
        {
            LOGGER.warn( "These warnings were in the document from {}: {}",
                         uri,
                         wrdsWarnings );
        }

        Map<String, String> variable = rootDocument.getVariable();

        if ( Objects.isNull( variable )
             || variable.isEmpty()
             || !variable.containsKey( "name" )
             || !variable.containsKey( "unit" ) )
        {
            throw new ReadException( "Invalid document from WRDS (variable"
                                     + " and/or unit missing): check the "
                                     + "WRDS and WRES documentation to "
                                     + "ensure the most up-to-date base "
                                     + "URL is declared in the source tag."
                                     + " The invalid document was from "
                                     + uri );
        }

        String variableName = variable.get( "name" );
        String measurementUnit = variable.get( "unit" );

        if ( Objects.isNull( variableName )
             || variableName.isBlank()
             || Objects.isNull( measurementUnit )
             || measurementUnit.isBlank() )
        {
            throw new ReadException( "Invalid document from WRDS (variable"
                                     + " and/or unit value was missing): "
                                     + "check the WRDS and WRES "
                                     + "documentation to ensure the most "
                                     + "up-to-date base URL is declared in"
                                     + " the source tag. The invalid "
                                     + "document was from "
                                     + uri );
        }

        return this.readFromNwmRootDocument( rootDocument, dataSource, variableName, measurementUnit );
    }

    /**
     * Transforms a root document of time-series into internal time-series.
     * @param rootDocument the root document
     * @param dataSource the data source
     * @param variableName the variable name
     * @param measurementUnit the measurement unit
     * @return the internal time-series
     */
    private List<TimeSeriesTuple> readFromNwmRootDocument( NwmRootDocument rootDocument,
                                                           DataSource dataSource,
                                                           String variableName,
                                                           String measurementUnit )
    {
        URI uri = dataSource.getUri();

        // Time scale if available
        TimeScaleOuter timeScale = null;

        if ( Objects.nonNull( rootDocument.getParameterCodes() ) )
        {
            timeScale = TimeScaleFromParameterCodes.getTimeScale( rootDocument.getParameterCodes(), uri );
            LOGGER.debug( "While processing source {} discovered a time scale of {}.",
                          uri,
                          timeScale );
        }

        List<TimeSeriesTuple> timeSeriesTuples = new ArrayList<>();

        // Iterate through the results
        for ( NwmForecast forecast : rootDocument.getForecasts() )
        {
            for ( NwmFeature nwmFeature : forecast.getFeatures() )
            {
                TimeSeriesTuple tuple = this.getTimeSeries( dataSource,
                                                            forecast,
                                                            nwmFeature,
                                                            timeScale,
                                                            variableName,
                                                            measurementUnit );

                timeSeriesTuples.add( tuple );
            }
        }

        return Collections.unmodifiableList( timeSeriesTuples );
    }

    /**
     * @param dataSource the data source
     * @param forecast the nwm forecast
     * @param nwmFeature the nwm feature
     * @param timeScale the time scale, if available
     * @param variableName the variable name
     * @param measurementUnit the measurement unit
     * @return the internal time-series
     */

    private TimeSeriesTuple getTimeSeries( DataSource dataSource,
                                           NwmForecast forecast,
                                           NwmFeature nwmFeature,
                                           TimeScaleOuter timeScale,
                                           String variableName,
                                           String measurementUnit )
    {
        URI uri = dataSource.getUri();

        // Read into an intermediate structure
        SortedMap<String, SortedMap<Instant, Double>> traces = new TreeMap<>();

        int rawLocationId = nwmFeature.getLocation()
                                      .getNwmLocationNames()
                                      .getNwmFeatureId();
        NwmMember[] members = nwmFeature.getMembers();

        for ( int i = 0; i < members.length; i++ )
        {
            SortedMap<Instant, Double> values = new TreeMap<>();

            for ( NwmDataPoint dataPoint : members[i].getDataPoints() )
            {
                if ( Objects.isNull( dataPoint ) )
                {
                    LOGGER.debug( "Found null datapoint in member trace={} at referenceDatetime={} for nwm feature={}.",
                                  i,
                                  forecast.getReferenceDatetime(),
                                  rawLocationId );
                    continue;
                }

                Instant validTime = dataPoint.time();
                double member = dataPoint.value();

                if ( values.containsKey( validTime ) )
                {
                    throw new ReadException( "Discovered a time-series with duplicate valid datetimes, which is not "
                                             + "allowed. The duplicate occurred for reference time "
                                             + forecast.getReferenceDatetime()
                                             + ", valid time "
                                             + validTime
                                             + " and trace name "
                                             + members[i].getIdentifier()
                                             + "." );
                }

                values.put( validTime, member );
            }

            traces.put( members[i].getIdentifier(), values );
        }

        ReferenceTimeType referenceTimeType = ReferenceTimeType.T0;

        // Special rule: when analysis data is found, reference time not T0.
        if ( uri.getPath()
                .toLowerCase()
                .contains( "analysis" ) )
        {
            referenceTimeType = ReferenceTimeType.ANALYSIS_START_TIME;

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Analysis data found labeled in URI {}", uri );
            }
        }

        String locationId = Integer.toString( rawLocationId );
        Geometry geometry = MessageFactory.getGeometry( locationId );
        Feature feature = Feature.of( geometry );
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( referenceTimeType,
                                                                     forecast.getReferenceDatetime() ),
                                                             timeScale,
                                                             variableName,
                                                             feature,
                                                             measurementUnit );
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
            ReaderUtilities.validateAgainstEmptyTimeSeries( series, dataSource.getUri() );

            return TimeSeriesTuple.ofSingleValued( series, dataSource );
        }
        // Ensemble
        else if ( members.length > 1 )
        {
            TimeSeries<Ensemble> series = ReaderUtilities.transformEnsemble( metadata, traces, -1, uri );

            LOGGER.debug( "Read an ensemble NWM time-series from the root document, which contained {} events.",
                          series.getEvents()
                                .size() );

            // Validate
            ReaderUtilities.validateAgainstEmptyTimeSeries( series, dataSource.getUri() );

            return TimeSeriesTuple.ofEnsemble( series, dataSource );
        }
        // No members
        else
        {
            throw new ReadException( "While attempting to read NWM time-series data in WRDS JSON format for feature "
                                     + nwmFeature.getLocation().getNwmLocationNames().getNwmFeatureId()
                                     + ", encountered zero traces, which is not allowed." );
        }
    }

    /**
     * Reads the root document from a stream.
     * @param inputStream the stream
     * @param uri the origin of the stream
     * @return the root document
     */
    private NwmRootDocument getNwmRootDocument( InputStream inputStream, URI uri )
    {
        LOGGER.debug( "Reading a WRDS NWM source from {}.", uri );

        try
        {
            NwmRootDocument document = OBJECT_MAPPER.readValue( inputStream, NwmRootDocument.class );
            LOGGER.debug( "Parsed this document: {}", document );
            return document;
        }
        catch ( IOException ioe )
        {
            throw new ReadException( "Failed to read NWM data from "
                                     + uri
                                     + ".",
                                     ioe );
        }
    }

    /**
     * Hidden constructor.
     */

    private WrdsNwmJsonReader()
    {
    }
}

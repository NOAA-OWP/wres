package wres.reading.wrds.hefs;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.TimeSeries;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;

/**
 * <p>Reads time-series data from the U.S. National Weather Service (NWS) Hydrologic Ensemble Forecast Service (HEFS)
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
 * <p>TODO: consider a more memory efficient implementation by using the Jackson streaming API. For example:
 * <a href="https://www.baeldung.com/jackson-streaming-api">Jackson</a>. As of v6.7, this is not a tremendous problem
 * because the main application of this class is reading directly from WRDS whereby the responses are chunked at a
 * higher level. However, this limitation would become more acute were there a need to read a large WRDS-formatted
 * JSON file from a local disk.
 *
 * @author James Brown
 */

public class WrdsHefsJsonReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsHefsJsonReader.class );

    /** Maps JSON bytes to POJOs. */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES );

    /**
     * @return an instance
     */

    public static WrdsHefsJsonReader of()
    {
        return new WrdsHefsJsonReader();
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
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.JSON_WRDS_HEFS );

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
                                          dataSource.uri() );
                         }
                     } );
    }

    /**
     * <p>Returns a time-series supplier from the inputs. Currently, this method performs eager reading.
     *
     * <p> TODO: implement incremental reading using the Jackson Streaming API or similar.
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

            // Read all the time-series eagerly on first use: this will still delay any read until a terminal stream
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
                int index = iterator.getAndIncrement();
                return tuples.get( index );
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
        URI uri = dataSource.uri();

        try
        {
            byte[] rawBytes = inputStream.readAllBytes();

            if ( Objects.isNull( rawBytes ) )
            {
                return Collections.emptyList();
            }

            // It is conceivable that we could tee/pipe the data to both the md5sum and the parser at the same time,
            // but this involves more complexity and may not be worth it. For now assume that we are not going to
            // exhaust our heap by including the whole dataset here in memory temporarily.

            if ( LOGGER.isDebugEnabled() )
            {
                int byteCount = Math.min( 2048, rawBytes.length );
                byte[] firstBytes = new byte[byteCount];
                System.arraycopy( rawBytes, 0, firstBytes, 0, byteCount );
                LOGGER.debug( "First {} bytes of time series from {} as UTF-8: {}",
                              byteCount,
                              uri,
                              new String( firstBytes,
                                          StandardCharsets.UTF_8 ) );
            }

            HefsForecast[] forecasts = OBJECT_MAPPER.readValue( rawBytes, HefsForecast[].class );

            return Arrays.stream( forecasts )
                         .map( f -> TimeSeriesTuple.ofEnsemble( f.timeSeries(), dataSource ) )
                         .toList();
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read time-series from a WRDS HEFS data source. The "
                                     + "data source is: " + dataSource + ".", e );
        }
    }

    /**
     * Hidden constructor.
     */

    private WrdsHefsJsonReader()
    {
    }
}

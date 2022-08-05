package wres.io.reading.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.TimeSeriesTuple;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.waterml.WatermlReader;

/**
 * Reads time-series data from the National Water Information System (NWIS) Instantaneous Values (IV) web service. This 
 * service provides access to observed time-series whose event values always have an instantaneous time scale. The
 * service and its API is described here:
 * 
 * <p><a href="https://waterservices.usgs.gov/rest/IV-Service.html">USGS NWIS IV Web Service</a> 
 * 
 * <p>The above link was last accessed: 20220804T11:00Z.
 * 
 * <p>Implementation notes:
 * 
 * <p>This implementation reads time-series data in WaterML format only. The NWIS IV Service does support other formats, 
 * but WaterML is the default.
 * 
 * TODO: Currently, it is assumed that chunking of time-series data is performed at a higher level of abstraction by the 
 * {@link WebSource}. However, the service-specific logic in that class should be migrated here.
 *  
 * @author James Brown
 */

public class NwisReader implements TimeSeriesReader
{
    /** The underlying format reader. */
    private static final WatermlReader WATERML_READER = WatermlReader.of();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwisReader.class );

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient();

    /**
     * @return an instance
     */

    public static NwisReader of()
    {
        return new NwisReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        InputStream stream = NwisReader.getByteStreamFromUri( dataSource.getUri() );
        return this.read( dataSource, stream );
    }

    /**
     * This implementation is equivalent to calling {@link WatermlReader#read(DataSource, InputStream)}.
     * @param dataSource the data source, required
     * @param stream the input stream, required
     * @return the stream of time-series
     * @throws NullPointerException if the dataSource is null
     * @throws ReadException if the reading fails for any other reason
     */

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream stream )
    {
        LOGGER.debug( "Discovered an existing stream, assumed to be from a USGS NWIS IV service instance. Passing "
                      + "through to an underlying WaterML reader." );

        return WATERML_READER.read( dataSource, stream );
    }

    /**
     * Returns a byte stream from a file or web source.
     * 
     * @param uri the uri
     * @return the byte stream
     * @throws UnsupportedOperationException if the uri scheme is not one of http(s) or file
     * @throws ReadException if the stream could not be created for any other reason
     */

    private static InputStream getByteStreamFromUri( URI uri )
    {
        Objects.requireNonNull( uri );

        try
        {
            if ( uri.getScheme().toLowerCase().startsWith( "http" ) )
            {
                try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri ) )
                {
                    int httpStatus = response.getStatusCode();

                    if ( httpStatus == 404 )
                    {
                        LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                                     httpStatus,
                                     uri );

                        return null;
                    }
                    else if ( ! ( httpStatus >= 200 && httpStatus < 300 ) )
                    {
                        throw new ReadException( "Failed to read data from '"
                                                 + uri
                                                 +
                                                 "' due to HTTP status code "
                                                 + httpStatus );
                    }

                    if ( LOGGER.isTraceEnabled() )
                    {
                        byte[] rawData = IOUtils.toByteArray( response.getResponse() );

                        LOGGER.trace( "Response body for {}: {}",
                                      uri,
                                      new String( rawData,
                                                  StandardCharsets.UTF_8 ) );
                    }

                    return response.getResponse();
                }
            }
            else
            {
                throw new ReadException( "Cannot read USGS NWIS time-series data from "
                                         + uri
                                         + ". Did you intend to use a WaterML reader?" );
            }
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to acquire a byte stream from "
                                     + uri
                                     + ".",
                                     e );
        }
    }

    /**
     * Hidden constructor.
     */

    private NwisReader()
    {
    }

}

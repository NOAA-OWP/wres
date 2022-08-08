package wres.io.reading.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import wres.datamodel.time.TimeSeriesTuple;
import wres.io.ingesting.PreIngestException;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.waterml.WatermlReader;
import wres.io.reading.wrds.nwm.NwmRootDocumentWithError;
import wres.io.reading.wrds.nwm.WrdsNwmJsonReader;

/**
 * Reads time-series data from the National Weather Service (NWS) Water Resources Data Service for the National Water 
 * Model (NWM).
 * 
 * TODO: Currently, it is assumed that chunking of time-series data is performed at a higher level of abstraction by the 
 * {@link WebSource}. However, the service-specific logic in that class should be migrated here.
 *  
 * @author James Brown
 */

public class WrdsNwmReader implements TimeSeriesReader
{
    /** The underlying format reader for JSON-formatted data from the NWM service. */
    private static final WrdsNwmJsonReader NWM_READER = WrdsNwmJsonReader.of();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReader.class );

    /** Trust manager for TLS connections to the WRDS services. */
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT;

    static
    {
        try
        {
            SSL_CONTEXT = ReaderUtilities.getSslContextTrustingDodSignerForWrds();
        }
        catch ( PreIngestException e )
        {
            throw new ExceptionInInitializerError( "Failed to acquire the TLS context for connecting to WRDS: "
                                                   + e.getMessage() );
        }
    }

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT );

    /**
     * @return an instance
     */

    public static WrdsNwmReader of()
    {
        return new WrdsNwmReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        InputStream stream = WrdsNwmReader.getByteStreamFromDataSource( dataSource );
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
        LOGGER.debug( "Discovered an existing stream, assumed to be from a WRDS NWM service instance. Passing through "
                + "to an underlying WARDS NWM JSON reader." );

        return NWM_READER.read( dataSource, stream );
    }

    /**
     * Returns a byte stream from a file or web source.
     * 
     * @param dataSource the the data source
     * @return the byte stream
     * @throws UnsupportedOperationException if the uri scheme is not one of http(s) or file
     * @throws ReadException if the stream could not be created for any other reason
     */

    private static InputStream getByteStreamFromDataSource( DataSource dataSource )
    {
        URI uri = dataSource.getUri();

        Objects.requireNonNull( uri );

        if ( uri.getScheme().toLowerCase().startsWith( "http" ) )
        {
            try
            {
                // Stream us closed at a higher level
                WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri );
                int httpStatus = response.getStatusCode();

                // Read an error if possible
                if ( httpStatus >= 400 && httpStatus < 500 )
                {
                    String possibleError = WrdsNwmReader.tryToReadErrorMessage( response.getResponse() );

                    if ( Objects.nonNull( possibleError ) )
                    {
                        LOGGER.warn( "Found this WRDS error message from URI {}: {}",
                                     uri,
                                     possibleError );
                    }

                    return null;
                }

                return response.getResponse();
            }
            catch ( IOException e )
            {
                throw new ReadException( "Failed to acquire a byte stream from "
                                         + uri
                                         + ".",
                                         e );
            }
        }
        else
        {
            throw new ReadException( "Unable to read WRDS source " + uri
                                     + "because it does not use the http "
                                     + "scheme. Did you intend to use a JSON reader?" );
        }
    }

    /**
     * Attempt to read an error message from the WRDS NWM service for a document like this:
     * {
     *   "error": "API Currently only supports querying by the following: ('nwm_feature_id', 'nws_lid', ... )"
     * }
     *
     * If anything goes wrong, returns null.
     *
     * @param inputStream the stream containing a potential error message
     * @return the value from the above map, null if not found.
     */

    private static String tryToReadErrorMessage( InputStream inputStream )
    {
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            NwmRootDocumentWithError document = mapper.readValue( inputStream,
                                                                  NwmRootDocumentWithError.class );
            Map<String, String> messages = document.getMessages();

            if ( Objects.nonNull( messages ) )
            {
                return messages.get( "error" );
            }
        }
        catch ( IOException ioe )
        {
            LOGGER.debug( "Failed to parse an error response body.", ioe );
        }

        return null;
    }

    /**
     * Hidden constructor.
     */

    private WrdsNwmReader()
    {
    }

}

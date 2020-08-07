package wres.io.geography;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import wres.io.geography.wrds.WrdsLocationRootDocument;
import wres.io.reading.PreIngestException;
import wres.io.reading.wrds.ReadValueManager;
import wres.io.utilities.WebClient;

public class WrdsLocationReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsLocationReader.class );
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT = ReadValueManager.getSslContextTrustingDodSigner();
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT );
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private WrdsLocationReader()
    {
        // Hidden, no instance state needed. If state is needed here, add it.
    }

    /**
     * Read a WRDS location document for a single location.
     * @param uri Either a file or a web uri, absolute, including scheme.
     * @return The WRDS location document in readable/immutable POJO form.
     * @throws IllegalArgumentException When uri is not absolute, has no scheme.
     * @throws NullPointerException When uri is null.
     * @throws PreIngestException When getting, reading, or parsing fails.
     */

    public static WrdsLocationRootDocument read( URI uri )
    {
        Objects.requireNonNull( uri );

        if ( !uri.isAbsolute() )
        {
            throw new IllegalArgumentException( "URI passed must be absolute, not "
                                                + uri );
        }

        if ( Objects.isNull( uri.getScheme() ) )
        {
            throw new IllegalArgumentException( "URI passed must have scheme, not "
                                                + uri );
        }

        LOGGER.info(" Getting location data from {}", uri );
        byte[] rawResponseBytes;

        if ( uri.getScheme()
                .toLowerCase()
                .equals( "file" ) )
        {
            rawResponseBytes = readFromFile( uri );
        }
        else
        {
            rawResponseBytes = readFromWeb( uri );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Raw response, decoded as UTF-8: {}",
                          new String( rawResponseBytes, UTF_8 ) );
        }

        try
        {
            return OBJECT_MAPPER.readValue( rawResponseBytes,
                                            WrdsLocationRootDocument.class );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to parse document from "
                                          + uri );
        }
    }

    private static byte[] readFromWeb( URI uri )
    {
        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri ) )
        {
            if ( response.getStatusCode() != 200 )
            {
                throw new PreIngestException( "Failed to read location data from "
                                              + uri + " due to HTTP status "
                                              + response.getStatusCode() );
            }

            return response.getResponse()
                           .readAllBytes();
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Unable to read location data from web at "
                                          + uri, ioe );
        }
    }

    private static byte[] readFromFile( URI uri )
    {
        try ( InputStream response = new FileInputStream( new File( uri ) ) )
        {
            return response.readAllBytes();
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Unable to read location data from file at "
                                          + uri, ioe );
        }
    }
}

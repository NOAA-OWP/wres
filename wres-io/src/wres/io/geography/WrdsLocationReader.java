package wres.io.geography;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import wres.io.geography.wrds.WrdsLocation;
import wres.io.geography.wrds.WrdsLocationRootDocument;
import wres.io.geography.wrds.WrdsLocationRootDocumentV3;
import wres.io.geography.wrds.version.WrdsLocationRootVersionDocument;
import wres.io.reading.PreIngestException;
import wres.io.reading.wrds.ReadValueManager;
import wres.io.utilities.WebClient;

public class WrdsLocationReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsLocationReader.class );
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT =
            ReadValueManager.getSslContextTrustingDodSigner();
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT );
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private WrdsLocationReader()
    {
        // Hidden, no instance state needed. If state is needed here, add it.
    }

    public static List<WrdsLocation> read( URI uri )
    {
        byte[] rawResponseBytes = getRawResponseBytes( uri );

        //Get the version information
        WrdsLocationRootVersionDocument versionDoc;
        try
        {
            versionDoc = OBJECT_MAPPER.readValue( rawResponseBytes, WrdsLocationRootVersionDocument.class );
        }
        catch ( IOException e )
        {
            throw new PreIngestException( "Failed to parse API version information from "
                                          + uri );
        }

        try
        {
            if ( versionDoc.isDeploymentInfoPresent() )
            {
                WrdsLocationRootDocumentV3 doc = OBJECT_MAPPER.readValue( rawResponseBytes,
                                                                          WrdsLocationRootDocumentV3.class );
                return doc.getLocations();
            }
            else
            {
                WrdsLocationRootDocument doc = OBJECT_MAPPER.readValue( rawResponseBytes,
                                                                        WrdsLocationRootDocument.class );
                return doc.getLocations();
            }
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to parse location information from document from "
                                          + uri );
        }

    }


    private static byte[] getRawResponseBytes( URI uri )
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

        LOGGER.info( " Getting location data from {}", uri );
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

        return rawResponseBytes;
    }


    private static byte[] readFromWeb( URI uri )
    {
        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri ) )
        {
            if ( response.getStatusCode() != 200 )
            {
                throw new PreIngestException( "Failed to read location data from "
                                              + uri
                                              + " due to HTTP status "
                                              + response.getStatusCode() );
            }

            return response.getResponse()
                           .readAllBytes();
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Unable to read location data from web at "
                                          + uri,
                                          ioe );
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
                                          + uri,
                                          ioe );
        }
    }
}

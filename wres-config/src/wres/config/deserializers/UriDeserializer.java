package wres.config.deserializers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes a {@link URI}, performing any platform-dependent disambiguation.
 *
 * @author James Brown
 */
public class UriDeserializer extends ValueDeserializer<URI>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( UriDeserializer.class );

    @Override
    public URI deserialize( JsonParser p, DeserializationContext ctxt )
    {
        ObjectReadContext mapper = p.objectReadContext();
        JsonNode node = mapper.readTree( p );

        String uriText = node.asString();

        return UriDeserializer.deserializeUri( uriText );
    }

    /**
     * Interpolates a URI, adding a file scheme where needed, resolving relative paths and performing any
     * platform-dependent disambiguation.
     * @param uriString the URI string
     * @return the adjusted URI
     */

    static URI deserializeUri( String uriString )
    {
        try
        {
            // Web-like URI? If so, return as-is
            if ( uriString.toLowerCase()
                          .startsWith( "http" ) )
            {
                LOGGER.debug( "Not adjusting URI: {}.", uriString );
                return new URI( uriString );
            }

            // For Windows paths, tolerate a backslash by automatically converting
            // all backslashes into slashes.
            String noBackslashes = uriString.replace( '\\', '/' );

            // Windows has a drive letter which can be misinterpreted as a
            // provider portion of a URI which is subsequently not found.
            // When Windows is detected and the provider is a single ascii
            // letter from A to Z, insert the file provider into the URI before
            // attempting to convert to a Path.
            // This technique is a guess, but there are no official single digit
            // schemes as of 2019-10-30, so it's a pretty good guess.
            // See https://www.iana.org/assignments/uri-schemes/uri-schemes.xhtml
            // The only reason to have this hack is to not have to explain URIs to
            // users and force users to use "file:/C:/data/path" instead of
            // "C:/data/path", and yet use the "scheme" as a hint below as to
            // whether this is a file or some other scheme/protocol/whatever.
            if ( Objects.nonNull( System.getProperty( "os.name" ) )
                 && System.getProperty( "os.name" )
                          .toLowerCase()
                          .contains( "windows" )
                 && noBackslashes.length() > 2
                 && noBackslashes.charAt( 1 ) == ':' )
            {
                LOGGER.debug( "Windows has been detected and possible URI {} probably starts with a drive letter.",
                              noBackslashes );
                char drive = noBackslashes.toUpperCase()
                                          .charAt( 0 );
                // Ascii A through Z is 65 through 90.
                if ( drive >= 65 && drive <= 90 )
                {
                    LOGGER.debug( "Windows and URI with drive letter {} found.",
                                  drive );
                    noBackslashes = "file:/" + noBackslashes;
                }
                else
                {
                    LOGGER.debug( "Windows but URI had no drive letter: {}",
                                  drive );
                }
            }

            return new URI( noBackslashes );
        }
        catch ( URISyntaxException e )
        {
            LOGGER.debug( "The declaration contained an invalid URI.", e );

            // Return a null sentinel, indicating that the URI failed validation
            return null;
        }
    }
}

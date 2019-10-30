package wres.config;

import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts text from xml tag into a URI, or null if it cannot be converted.
 *
 * Automatically replaces backslashes with slashes for convenience on Windows.
 *
 * The exception generated during unmarshal should be converted to a
 * ValidationEvent that is reported after full unmarshal. This does in fact
 * happen, but the location of the issue is lost and reported as line -1 and
 * column -1. The previous solution (using URI.create in the .xjb file) was
 * somewhat less human-written code, and therefore simpler, but the convenience
 * of replacing backslashes with slashes may make this class worthwhile. If it
 * is not worthwhile, feel free to remove this class.
 */

public class UriAdapter extends XmlAdapter<String,URI>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( UriAdapter.class );

    @Override
    public URI unmarshal( String possibleURI )
    {
        // For Windows paths, tolerate a backslash by automatically converting
        // all backslashes into slashes.
        String noBackslashes = possibleURI.replace( '\\', '/' );

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
        if ( System.getProperty( "os.name" ) != null
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

        try
        {
            return new URI( noBackslashes );
        }
        catch ( URISyntaxException e )
        {
            String message = "Failed to convert text '"
                             + noBackslashes + "' to a URI. (Original text '"
                             + possibleURI + "')";
            throw new ProjectConfigException( null, message, e );
        }
    }

    @Override
    public String marshal( URI existingURI )
    {
        return existingURI.toASCIIString();
    }
}

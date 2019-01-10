package wres.config;

import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.bind.annotation.adapters.XmlAdapter;

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
    @Override
    public URI unmarshal( String possibleURI )
    {
        // For Windows paths, tolerate a backslash by automatically converting
        // all backslashes into slashes.
        String noBackslashes = possibleURI.replace( '\\', '/' );

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

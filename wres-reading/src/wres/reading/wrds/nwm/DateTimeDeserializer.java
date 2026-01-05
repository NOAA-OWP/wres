package wres.reading.wrds.nwm;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;

import wres.reading.ReadException;
import wres.reading.ReaderUtilities;

/**
 * Custom deserializer for a datetime string in the ISO8601 "basic" format with optional minutes and seconds. For
 * example: 20240830T12Z, 20240830T1200Z and 20240830T120000Z are all acceptable.
 *
 * @author James Brown
 */
public class DateTimeDeserializer extends ValueDeserializer<Instant>
{

    @Override
    public Instant deserialize( JsonParser jp, DeserializationContext context )
    {
        JsonNode node = jp.objectReadContext()
                          .readTree( jp );

        String time;

        // Parse the instant.
        if ( node.isString() )
        {
            time = node.asString();
        }
        else
        {
            throw new ReadException( "Could not find a datetime field in the document, which is not allowed." );
        }

        // Lenient formatting in the "basic" ISO8601 format, hours and seconds are optional
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern( "[yyyyMMdd'T'HH[:mm[:ss]]'Z'][yyyy-MM-dd'T'HH:mm:ss'Z']" )
                                 .withZone( ReaderUtilities.UTC );

        return formatter.parse( time, Instant::from );
    }
}
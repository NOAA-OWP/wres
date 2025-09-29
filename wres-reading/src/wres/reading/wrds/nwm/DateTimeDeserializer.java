package wres.reading.wrds.nwm;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.reading.ReaderUtilities;

/**
 * Custom deserializer for a datetime string in the ISO8601 "basic" format with optional minutes and seconds. For
 * example: 20240830T12Z, 20240830T1200Z and 20240830T120000Z are all acceptable.
 *
 * @author James Brown
 */
public class DateTimeDeserializer extends JsonDeserializer<Instant>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DateTimeDeserializer.class );

    @Override
    public Instant deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        JsonNode node = jp.getCodec()
                          .readTree( jp );

        String time;

        // Parse the instant.
        if ( node.isTextual() )
        {
            time = node.asText();
        }
        else
        {
            throw new IOException( "Could not find a datetime field in the document, which is not allowed." );
        }

        // Lenient formatting in the "basic" ISO8601 format, hours and seconds are optional
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "[yyyyMMdd'T'HH[:mm[:ss]]'Z'][yyyy-MM-dd'T'HH:mm:ss'Z']" )
                                                       .withZone( ReaderUtilities.UTC );
        return formatter.parse( time, Instant::from );
    }
}
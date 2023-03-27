package wres.config.yaml.deserializers;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Custom deserializer for a {@link ZoneId} that admits several informal shorthands, as well as all named time zones
 * accepted by {@link ZoneId#of(String)}.
 *
 * @author James Brown
 */
public class ZoneOffsetDeserializer extends JsonDeserializer<ZoneOffset>
{
    /** Time zone shorthands, which are currently CONUS-centric. */
    private static final Map<String, ZoneOffset> SHORTHANDS =
            Map.ofEntries( Map.entry( "UTC", ZoneOffset.of( "+0000" ) ),
                           Map.entry( "GMT", ZoneOffset.of( "+0000" ) ),
                           Map.entry( "EDT", ZoneOffset.of( "-0400" ) ),
                           Map.entry( "EST", ZoneOffset.of( "-0500" ) ),
                           Map.entry( "CDT", ZoneOffset.of( "-0500" ) ),
                           Map.entry( "CST", ZoneOffset.of( "-0600" ) ),
                           Map.entry( "MDT", ZoneOffset.of( "-0600" ) ),
                           Map.entry( "MST", ZoneOffset.of( "-0700" ) ),
                           Map.entry( "PDT", ZoneOffset.of( "-0700" ) ),
                           Map.entry( "PST", ZoneOffset.of( "-0800" ) ),
                           Map.entry( "AKDT", ZoneOffset.of( "-0800" ) ),
                           Map.entry( "AKST", ZoneOffset.of( "-0900" ) ),
                           Map.entry( "HADT", ZoneOffset.of( "-0900" ) ),
                           Map.entry( "HAST", ZoneOffset.of( "-1000" ) ) );

    @Override
    public ZoneOffset deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        String zoneText = node.asText();

        // Shorthand? If so, allow
        if ( SHORTHANDS.containsKey( zoneText ) )
        {
            return SHORTHANDS.get( zoneText );
        }
        // Otherwise, pass through directly
        else
        {
            return ZoneOffset.of( zoneText );
        }
    }
}


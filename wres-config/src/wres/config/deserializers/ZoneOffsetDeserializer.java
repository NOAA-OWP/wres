package wres.config.deserializers;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.DateTimeException;
import java.util.Map;
import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationException;

/**
 * Custom deserializer for a {@link ZoneId} that admits several informal shorthands, as well as all named time zones
 * accepted by {@link ZoneId#of(String)}.
 *
 * @author James Brown
 */
public class ZoneOffsetDeserializer extends ValueDeserializer<ZoneOffset>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ZoneOffsetDeserializer.class );

    /** UTC. */
    private static final ZoneOffset UTC = ZoneOffset.of( "+0000" );

    /** UTC - 4H. */
    private static final ZoneOffset M4H = ZoneOffset.of( "-0400" );

    /** UTC - 5H. */
    private static final ZoneOffset M5H = ZoneOffset.of( "-0500" );

    /** UTC - 6H. */
    private static final ZoneOffset M6H = ZoneOffset.of( "-0600" );

    /** UTC - 7H. */
    private static final ZoneOffset M7H = ZoneOffset.of( "-0700" );

    /** UTC - *H. */
    private static final ZoneOffset M8H = ZoneOffset.of( "-0800" );

    /** UTC - 9H. */
    private static final ZoneOffset M9H = ZoneOffset.of( "-0900" );

    /** UTC - 10H. */
    private static final ZoneOffset M10H = ZoneOffset.of( "-1000" );

    /** Time zone shorthands, which are currently CONUS-centric. */
    private static final Map<String, ZoneOffset> SHORTHANDS =
            Map.ofEntries( Map.entry( "UTC", UTC ),
                           Map.entry( "GMT", UTC ),
                           Map.entry( "EDT", M4H ),
                           Map.entry( "EST", M5H ),
                           Map.entry( "CDT", M5H ),
                           Map.entry( "CST", M6H ),
                           Map.entry( "MDT", M6H ),
                           Map.entry( "MST", M7H ),
                           Map.entry( "PDT", M7H ),
                           Map.entry( "PST", M8H ),
                           Map.entry( "AKDT", M8H ),
                           Map.entry( "AKST", M9H ),
                           Map.entry( "HADT", M9H ),
                           Map.entry( "HAST", M10H ),
                           Map.entry( "utc", UTC ),
                           Map.entry( "gmt", UTC ),
                           Map.entry( "edt", M4H ),
                           Map.entry( "est", M5H ),
                           Map.entry( "cdt", M5H ),
                           Map.entry( "cst", M6H ),
                           Map.entry( "mdt", M6H ),
                           Map.entry( "mst", M7H ),
                           Map.entry( "pdt", M7H ),
                           Map.entry( "pst", M8H ),
                           Map.entry( "akdt", M8H ),
                           Map.entry( "akst", M9H ),
                           Map.entry( "hadt", M9H ),
                           Map.entry( "hast", M10H ) );

    @Override
    public ZoneOffset deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext mapper = jp.objectReadContext();
        JsonNode node = mapper.readTree( jp );

        String zoneText = node.asString();

        return ZoneOffsetDeserializer.getZoneOffset( zoneText );
    }

    /**
     * Creates a zone offset from a string.
     * @param zoneOffset the zone offset string
     * @return the zone offset
     * @throws DateTimeException if the offset could not be created
     */
    public static ZoneOffset getZoneOffset( String zoneOffset )
    {
        Objects.requireNonNull( zoneOffset );

        LOGGER.debug( "Discovered a 'time_zone_offset' of {} to deserialize.", zoneOffset );

        // Shorthand? If so, allow
        if ( SHORTHANDS.containsKey( zoneOffset ) )
        {
            return SHORTHANDS.get( zoneOffset );
        }
        // Otherwise, pass through directly
        else
        {
            try
            {
                return ZoneOffset.of( zoneOffset );
            }
            catch ( DateTimeException e )
            {
                throw new DeclarationException( "Failed to deserialize a 'time_zone_offset'. Please use a valid offset "
                                                + "name, such as 'EST' or a valid ISO 8601 UTC offset, such as "
                                                + "'-05:00'.",
                                                e );
            }
        }
    }
}


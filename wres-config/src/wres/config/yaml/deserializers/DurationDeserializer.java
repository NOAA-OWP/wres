package wres.config.yaml.deserializers;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Custom deserializer for a {@link Duration}.
 *
 * @author James Brown
 */
public class DurationDeserializer extends JsonDeserializer<Duration>
{
    @Override
    public Duration deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        return DurationDeserializer.getDuration( mapper, node, "period", "unit" );
    }

    /**
     * Reads a {@link Duration} from a {@link JsonNode}.
     * @param mapper the object mapper
     * @param node the node to read
     * @param durationNodeName the name of the node that contains the duration quantity
     * @param durationUnitName the duration unit name
     * @return a duration
     * @throws IOException if the node could not be read
     */

    static Duration getDuration( ObjectReader mapper,
                                 JsonNode node,
                                 String durationNodeName,
                                 String durationUnitName ) throws IOException
    {
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( node );

        Duration duration = null;
        if ( node.has( durationNodeName )
             && node.has( durationUnitName ) )
        {
            JsonNode periodNode = node.get( durationNodeName );
            JsonNode unitNode = node.get( durationUnitName );
            long durationUnit = periodNode.asLong();
            String unitString = unitNode.asText();
            ChronoUnit chronoUnit = mapper.readValue( unitString, ChronoUnit.class );
            duration = Duration.of( durationUnit, chronoUnit );
        }

        return duration;
    }
}
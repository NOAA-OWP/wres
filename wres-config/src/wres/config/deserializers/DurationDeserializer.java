package wres.config.deserializers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;

import wres.config.DeclarationFactory;

/**
 * Custom deserializer for a {@link Duration}.
 *
 * @author James Brown
 */
public class DurationDeserializer extends ValueDeserializer<Duration>
{
    @Override
    public Duration deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
        JsonNode node = reader.readTree( jp );

        return DurationDeserializer.getDuration( mapper, node, "period", "unit" );
    }

    /**
     * Reads a {@link Duration} from a {@link JsonNode}.
     * @param mapper the object mapper
     * @param node the node to read
     * @param durationNodeName the name of the node that contains the duration quantity
     * @param durationUnitName the duration unit name
     * @return a duration
     */

    static Duration getDuration( ObjectMapper mapper,
                                 JsonNode node,
                                 String durationNodeName,
                                 String durationUnitName )
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
            String unitString = unitNode.asString();
            ObjectReader reader = mapper.readerFor( ChronoUnit.class );
            ChronoUnit chronoUnit = reader.readValue( unitString );
            duration = Duration.of( durationUnit, chronoUnit );
        }

        return duration;
    }
}
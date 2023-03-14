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

        return DurationDeserializer.getDuration( mapper, node );
    }

    /**
     * Reads a {@link Duration} from a {@link JsonNode}.
     * @param mapper the object mapper
     * @param node the node to read
     * @return a duration
     * @throws IOException if the node could not be read
     */

    static Duration getDuration( ObjectReader mapper, JsonNode node ) throws IOException
    {
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( node );

        long period = 1;
        String unitString = "";
        if ( node.has( "period" ) )
        {
            JsonNode periodNode = node.get( "period" );
            period = periodNode.asLong();
        }

        if ( node.has( "unit" ) )
        {
            JsonNode unitNode = node.get( "unit" );
            unitString = unitNode.asText();
        }

        ChronoUnit chronoUnit = mapper.readValue( unitString, ChronoUnit.class );
        return Duration.of( period, chronoUnit );
    }
}


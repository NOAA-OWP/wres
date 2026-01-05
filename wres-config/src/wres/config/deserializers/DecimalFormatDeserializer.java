package wres.config.deserializers;

import java.text.DecimalFormat;
import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;

import wres.config.components.DecimalFormatPretty;

/**
 * Custom deserializer for a {@link DecimalFormat}.
 *
 * @author James Brown
 */
public class DecimalFormatDeserializer extends ValueDeserializer<DecimalFormat>
{
    @Override
    public DecimalFormat deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );
        ObjectReadContext mapper = jp.objectReadContext();
        JsonNode node = mapper.readTree( jp );
        return new DecimalFormatPretty( node.asString() );
    }
}


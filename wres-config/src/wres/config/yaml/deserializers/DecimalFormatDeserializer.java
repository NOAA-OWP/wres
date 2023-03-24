package wres.config.yaml.deserializers;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.components.DecimalFormatPretty;

/**
 * Custom deserializer for a {@link DecimalFormat}.
 *
 * @author James Brown
 */
public class DecimalFormatDeserializer extends JsonDeserializer<DecimalFormat>
{
    @Override
    public DecimalFormat deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );
        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );
        return new DecimalFormatPretty( node.asText() );
    }
}


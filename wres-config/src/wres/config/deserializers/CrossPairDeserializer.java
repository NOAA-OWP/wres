package wres.config.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.components.CrossPair;
import wres.config.components.CrossPairMethod;

/**
 * Custom deserializer for a {@link wres.config.components.CrossPair}.
 *
 * @author James Brown
 */
public class CrossPairDeserializer extends JsonDeserializer<CrossPair>
{
    @Override
    public CrossPair deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        // Single string
        if ( node.isTextual() )
        {
            CrossPairMethod method = reader.readValue( node, CrossPairMethod.class );
            return new CrossPair( method, null );
        }

        // Full declaration
        return reader.readValue( node, CrossPair.class );
    }
}


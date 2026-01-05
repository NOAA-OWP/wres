package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.CrossPair;

/**
 * Serializes a {@link CrossPair}.
 * @author James Brown
 */
public class CrossPairSerializer extends ValueSerializer<CrossPair>
{
    @Override
    public void serialize( CrossPair crossPair, JsonGenerator writer, SerializationContext serializers )
    {
        // Method only
        if ( Objects.isNull( crossPair.scope() ) )
        {
            writer.writePOJO( crossPair.method() );
        }
        // Full declaration, method and scope
        else
        {
            writer.writeStartObject();
            writer.writePOJOProperty( "method", crossPair.method() );
            writer.writePOJOProperty( "scope", crossPair.scope() );
            writer.writeEndObject();
        }
    }
}

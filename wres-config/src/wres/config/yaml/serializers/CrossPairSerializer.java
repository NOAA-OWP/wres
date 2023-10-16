package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.components.CrossPair;

/**
 * Serializes a {@link CrossPair}.
 * @author James Brown
 */
public class CrossPairSerializer extends JsonSerializer<CrossPair>
{
    @Override
    public void serialize( CrossPair crossPair, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        // Method only
        if( Objects.isNull( crossPair.scope() ) )
        {
            writer.writeObject( crossPair.method() );
        }
        // Full declaration, method and scope
        else
        {
            writer.writeStartObject();
            writer.writeObjectField( "method", crossPair.method() );
            writer.writeObjectField( "scope", crossPair.scope() );
            writer.writeEndObject();
        }
    }
}

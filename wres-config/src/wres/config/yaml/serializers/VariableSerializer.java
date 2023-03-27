package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.components.Variable;

/**
 * Serializes a {@link Variable}.
 * @author James Brown
 */
public class VariableSerializer extends JsonSerializer<Variable>
{
    @Override
    public void serialize( Variable variable, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        // If label is present, write object
        if( Objects.nonNull( variable.label() ) )
        {
            writer.writeObject( variable );
        }
        // Otherwise, write the name
        else
        {
            writer.writeString( variable.name() );
        }
    }
}

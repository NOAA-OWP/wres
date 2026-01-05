package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.Variable;

/**
 * Serializes a {@link Variable}.
 * @author James Brown
 */
public class VariableSerializer extends ValueSerializer<Variable>
{
    @Override
    public void serialize( Variable variable, JsonGenerator writer, SerializationContext serializers )
    {
        // If label is present, write object
        if ( Objects.nonNull( variable.label() ) )
        {
            writer.writePOJO( variable );
        }
        // Otherwise, write the name
        else
        {
            writer.writeString( variable.name() );
        }
    }
}

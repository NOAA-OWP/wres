package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

/**
 * Only serializes a {@link Boolean} value when <code>true</code>.
 * @author James Brown
 */
public class TrueSerializer extends ValueSerializer<Boolean>
{
    @Override
    public void serialize( Boolean value, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writeBoolean( value );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, Boolean value )
    {
        return !value;
    }
}

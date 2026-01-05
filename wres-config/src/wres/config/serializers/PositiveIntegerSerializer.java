package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

/**
 * Only serializes an {@link Integer} that is greater than zero.
 * @author James Brown
 */
public class PositiveIntegerSerializer extends ValueSerializer<Integer>
{
    @Override
    public void serialize( Integer integer, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writeNumber( integer );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, Integer integer )
    {
        // Do not write the default
        return integer == 0;
    }
}

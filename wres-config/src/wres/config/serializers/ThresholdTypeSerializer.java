package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationFactory;
import wres.config.components.ThresholdType;

/**
 * Only serializes a {@link ThresholdType} that is not default.
 * @author James Brown
 */
public class ThresholdTypeSerializer extends ValueSerializer<ThresholdType>
{
    @Override
    public void serialize( ThresholdType type, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writePOJO( type );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, ThresholdType type )
    {
        // Do not write the default
        return type == DeclarationFactory.DEFAULT_THRESHOLD_TYPE;
    }
}

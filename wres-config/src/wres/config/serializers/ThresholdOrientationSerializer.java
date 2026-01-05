package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationFactory;
import wres.config.components.ThresholdOrientation;

/**
 * Only serializes a {@link ThresholdOrientation} that is not default.
 * @author James Brown
 */
public class ThresholdOrientationSerializer extends ValueSerializer<ThresholdOrientation>
{
    @Override
    public void serialize( ThresholdOrientation orientation, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writePOJO( orientation );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, ThresholdOrientation orientation )
    {
        // Do not write the default
        return orientation == DeclarationFactory.DEFAULT_THRESHOLD_ORIENTATION;
    }
}

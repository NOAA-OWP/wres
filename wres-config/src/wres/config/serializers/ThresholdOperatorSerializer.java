package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationFactory;
import wres.config.components.ThresholdOperator;

/**
 * Only serializes a {@link ThresholdOperator} that is not default.
 * @author James Brown
 */
public class ThresholdOperatorSerializer extends ValueSerializer<ThresholdOperator>
{
    @Override
    public void serialize( ThresholdOperator operator, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writePOJO( operator );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, ThresholdOperator operator )
    {
        // Do not write the default
        return operator == DeclarationFactory.DEFAULT_THRESHOLD_OPERATOR;
    }
}

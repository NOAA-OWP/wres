package wres.config.yaml.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.ThresholdOperator;

/**
 * Only serializes a {@link ThresholdOperator} that is not default.
 * @author James Brown
 */
public class ThresholdOperatorSerializer extends JsonSerializer<ThresholdOperator>
{
    @Override
    public void serialize( ThresholdOperator operator, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        writer.writeObject( operator );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, ThresholdOperator operator )
    {
        // Do not write the default
        return operator == DeclarationFactory.DEFAULT_THRESHOLD_OPERATOR;
    }
}

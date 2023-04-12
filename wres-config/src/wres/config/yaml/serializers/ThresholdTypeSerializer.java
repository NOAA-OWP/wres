package wres.config.yaml.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.ThresholdType;

/**
 * Only serializes a {@link ThresholdType} that is not default.
 * @author James Brown
 */
public class ThresholdTypeSerializer extends JsonSerializer<ThresholdType>
{
    @Override
    public void serialize( ThresholdType type, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        writer.writeObject( type );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, ThresholdType type )
    {
        // Do not write the default
        return type == DeclarationFactory.DEFAULT_THRESHOLD_TYPE;
    }
}

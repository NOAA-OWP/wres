package wres.config.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.DeclarationFactory;
import wres.config.components.ThresholdOrientation;

/**
 * Only serializes a {@link ThresholdOrientation} that is not default.
 * @author James Brown
 */
public class ThresholdOrientationSerializer extends JsonSerializer<ThresholdOrientation>
{
    @Override
    public void serialize( ThresholdOrientation orientation, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        writer.writeObject( orientation );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, ThresholdOrientation orientation )
    {
        // Do not write the default
        return orientation == DeclarationFactory.DEFAULT_THRESHOLD_ORIENTATION;
    }
}

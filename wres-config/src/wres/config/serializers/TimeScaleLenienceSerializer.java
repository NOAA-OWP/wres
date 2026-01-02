package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.TimeScaleLenience;

/**
 * Serializes a {@link TimeScaleLenience}.
 * @author James Brown
 */
public class TimeScaleLenienceSerializer extends ValueSerializer<TimeScaleLenience>
{
    @Override
    public void serialize( TimeScaleLenience lenience, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writeString( lenience.toString() );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, TimeScaleLenience lenience )
    {
        return lenience == TimeScaleLenience.NONE;
    }
}

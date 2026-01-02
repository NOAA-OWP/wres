package wres.config.serializers;

import java.time.ZoneOffset;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

/**
 * Serializes a {@link ZoneOffset}, adding quotes for safety.
 * @author James Brown
 */
public class ZoneOffsetSerializer extends ValueSerializer<ZoneOffset>
{
    @Override
    public void serialize( ZoneOffset offset, JsonGenerator writer, SerializationContext serializers )
    {
        String safeString = offset.toString()
                                  .replace( ":", "" );
        writer.writeString( safeString );
    }
}

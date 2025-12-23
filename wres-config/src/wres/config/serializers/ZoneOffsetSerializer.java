package wres.config.serializers;

import java.io.IOException;
import java.time.ZoneOffset;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializes a {@link ZoneOffset}, adding quotes for safety.
 * @author James Brown
 */
public class ZoneOffsetSerializer extends JsonSerializer<ZoneOffset>
{
    @Override
    public void serialize( ZoneOffset offset, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        String safeString = offset.toString()
                                  .replace( ":", "" );
        writer.writeString( safeString );
    }
}

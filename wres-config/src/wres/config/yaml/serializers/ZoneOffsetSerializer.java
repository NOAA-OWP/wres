package wres.config.yaml.serializers;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.components.BaselineDataset;

/**
 * Serializes a {@link ZoneOffset} because the deserialization stack does not play nicely with its default string
 * representation.
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

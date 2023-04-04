package wres.config.yaml.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Only serializes a {@link Boolean} value when <code>true</code>.
 * @author James Brown
 */
public class TrueSerializer extends JsonSerializer<Boolean>
{
    @Override
    public void serialize( Boolean value, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        writer.writeBoolean( value );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, Boolean value )
    {
        return !value;
    }
}

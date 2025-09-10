package wres.config.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Only serializes an {@link Integer} that is greater than zero.
 * @author James Brown
 */
public class PositiveIntegerSerializer extends JsonSerializer<Integer>
{
    @Override
    public void serialize( Integer integer, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        writer.writeNumber( integer );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, Integer integer )
    {
        // Do not write the default
        return integer == 0;
    }
}

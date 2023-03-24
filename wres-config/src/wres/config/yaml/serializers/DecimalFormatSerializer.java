package wres.config.yaml.serializers;

import java.io.IOException;
import java.text.DecimalFormat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializes a decimal format string.
 * @author James Brown
 */
public class DecimalFormatSerializer extends JsonSerializer<DecimalFormat>
{
    @Override
    public void serialize( DecimalFormat value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        gen.writeString( value.toPattern() );
    }
}

package wres.config.yaml.serializers;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.components.DecimalFormatPretty;

/**
 * Serializes a decimal format string.
 * @author James Brown
 */
public class DecimalFormatSerializer extends JsonSerializer<DecimalFormat>
{
    @Override
    public void serialize( DecimalFormat value, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        writer.writeString( value.toPattern() );
    }
}

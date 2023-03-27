package wres.config.yaml.serializers;

import java.io.IOException;
import java.text.DecimalFormat;

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

    @Override
    public boolean isEmpty( SerializerProvider serializers, DecimalFormat value  )
    {
        // Do not serialize the default
        return DecimalFormatPretty.DEFAULT_FORMAT.equals( value.toPattern() );
    }
}

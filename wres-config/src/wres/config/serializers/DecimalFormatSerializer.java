package wres.config.serializers;

import java.text.DecimalFormat;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

/**
 * Serializes a decimal format string.
 * @author James Brown
 */
public class DecimalFormatSerializer extends ValueSerializer<DecimalFormat>
{
    @Override
    public void serialize( DecimalFormat value, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writeString( value.toPattern() );
    }
}

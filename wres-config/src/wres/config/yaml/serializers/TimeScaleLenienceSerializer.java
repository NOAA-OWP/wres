package wres.config.yaml.serializers;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.DecimalFormatPretty;
import wres.config.yaml.components.TimeScaleLenience;

/**
 * Serializes a {@link TimeScaleLenience}.
 * @author James Brown
 */
public class TimeScaleLenienceSerializer extends JsonSerializer<TimeScaleLenience>
{
    @Override
    public void serialize( TimeScaleLenience lenience, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        writer.writeString( lenience.toString() );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, TimeScaleLenience lenience  )
    {
        return lenience == TimeScaleLenience.NONE;
    }
}

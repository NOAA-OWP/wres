package wres.config.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.DeclarationFactory;

/**
 * Only serializes a {@link Double} when it does not match the {@link DeclarationFactory#DEFAULT_MISSING_VALUE}.
 * @author James Brown
 */
public class MissingValueSerializer extends JsonSerializer<Double>
{
    @Override
    public void serialize( Double value, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        writer.writeNumber( value );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, Double value )
    {
        return Objects.isNull( value )
               || Math.abs( value - DeclarationFactory.DEFAULT_MISSING_VALUE ) < 0.000001;
    }
}

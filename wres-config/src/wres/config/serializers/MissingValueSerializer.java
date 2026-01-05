package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationFactory;

/**
 * Only serializes a {@link Double} when it does not match the {@link DeclarationFactory#DEFAULT_MISSING_VALUE}.
 * @author James Brown
 */
public class MissingValueSerializer extends ValueSerializer<Double>
{
    @Override
    public void serialize( Double value, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writeNumber( value );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, Double value )
    {
        return Objects.isNull( value )
               || Math.abs( value - DeclarationFactory.DEFAULT_MISSING_VALUE ) < 0.000001;
    }
}

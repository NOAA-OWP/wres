package wres.config.serializers;

import java.time.temporal.ChronoUnit;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationUtilities;

/**
 * Serializes a {@link ChronoUnit}.
 * @author James Brown
 */
public class ChronoUnitSerializer extends ValueSerializer<ChronoUnit>
{
    @Override
    public void serialize( ChronoUnit unit, JsonGenerator writer, SerializationContext serializers )
    {
        String friendlyName = DeclarationUtilities.fromEnumName( unit.name() );
        writer.writeString( friendlyName );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, ChronoUnit unit )
    {
        // Do not serialize the default
        return ChronoUnit.SECONDS == unit;
    }
}

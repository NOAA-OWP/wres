package wres.config.yaml.serializers;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.DeclarationUtilities;

/**
 * Serializes a {@link ChronoUnit}.
 * @author James Brown
 */
public class ChronoUnitSerializer extends JsonSerializer<ChronoUnit>
{
    @Override
    public void serialize( ChronoUnit unit, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        String friendlyName = DeclarationUtilities.fromEnumName( unit.name() );
        writer.writeString( friendlyName );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, ChronoUnit unit )
    {
        // Do not serialize the default
        return ChronoUnit.SECONDS == unit;
    }
}

package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationUtilities;
import wres.statistics.generated.Pool;

/**
 * Serializes a {@link Pool.EnsembleAverageType}.
 * @author James Brown
 */
public class EnsembleAverageTypeSerializer extends ValueSerializer<Pool.EnsembleAverageType>
{
    @Override
    public void serialize( Pool.EnsembleAverageType type, JsonGenerator writer, SerializationContext serializers )
    {
        String friendlyName = DeclarationUtilities.fromEnumName( type.name() );
        writer.writeString( friendlyName );
    }
}

package wres.config.yaml.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.Pool;

/**
 * Serializes a {@link Pool.EnsembleAverageType}.
 * @author James Brown
 */
public class EnsembleAverageTypeSerializer extends JsonSerializer<Pool.EnsembleAverageType>
{
    @Override
    public void serialize( Pool.EnsembleAverageType type, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        String friendlyName = DeclarationFactory.getFriendlyName( type.name() );
        writer.writeString( friendlyName );
    }
}

package wres.config.deserializers;

import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;

import wres.config.DeclarationFactory;
import wres.config.components.CrossPair;
import wres.config.components.CrossPairMethod;

/**
 * Custom deserializer for a {@link wres.config.components.CrossPair}.
 *
 * @author James Brown
 */
public class CrossPairDeserializer extends ValueDeserializer<CrossPair>
{
    @Override
    public CrossPair deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();

        // Single string
        if ( node.isString() )
        {
            ObjectReader objectReader = mapper.readerFor( CrossPairMethod.class );
            CrossPairMethod method = objectReader.readValue( node );
            return new CrossPair( method, null );
        }

        // Full declaration
        ObjectReader objectReader = mapper.readerFor( CrossPair.class );
        return objectReader.readValue( node );
    }
}


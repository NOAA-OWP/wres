package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.components.SpatialMask;

/**
 * Custom deserializer for a spatial mask.
 *
 * @author James Brown
 */
public class SpatialMaskDeserializer extends JsonDeserializer<SpatialMask>
{
    @Override
    public SpatialMask deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        if ( node.has( "wkt" ) )
        {
            return mapper.readValue( node, SpatialMask.class );
        }

        String wktString = node.asText();
        return new SpatialMask( null, wktString, null );
    }
}

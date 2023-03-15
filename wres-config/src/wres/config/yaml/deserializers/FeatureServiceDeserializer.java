package wres.config.yaml.deserializers;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceGroup;

/**
 * Custom deserializer for a {@link FeatureServiceGroup}.
 *
 * @author James Brown
 */
public class FeatureServiceDeserializer extends JsonDeserializer<FeatureService>
{
    @Override
    public FeatureService deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        // Preserve insertion order
        Set<FeatureServiceGroup> groups = new LinkedHashSet<>();
        URI uri = null;

        if ( node.has( "uri" ) )
        {
            String uriString = node.get( "uri" )
                                   .asText();
            uri = URI.create( uriString );
        }

        // Explicit groups
        if ( node.has( "groups" ) )
        {
            JsonNode groupNode = node.get( "groups" );
            int count = groupNode.size();
            for ( int i = 0; i < count; i++ )
            {
                JsonNode nextNode = groupNode.get( i );
                FeatureServiceGroup nextGroup = mapper.readValue( nextNode, FeatureServiceGroup.class );
                groups.add( nextGroup );
            }
        }
        // Singleton group
        else
        {
            FeatureServiceGroup singleton = mapper.readValue( node, FeatureServiceGroup.class );
            groups.add( singleton );
        }

        return new FeatureService( uri, Collections.unmodifiableSet( groups ) );
    }
}

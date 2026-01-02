package wres.config.deserializers;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.node.StringNode;

import wres.config.DeclarationFactory;
import wres.config.components.FeatureService;
import wres.config.components.FeatureServiceGroup;

/**
 * Custom deserializer for a {@link FeatureServiceGroup}.
 *
 * @author James Brown
 */
public class FeatureServiceDeserializer extends ValueDeserializer<FeatureService>
{
    @Override
    public FeatureService deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
        ObjectReader objectReader = mapper.readerFor( FeatureServiceGroup.class );

        // Preserve insertion order
        Set<FeatureServiceGroup> groups = new LinkedHashSet<>();
        URI uri = null;

        // Implicit URI
        if ( node instanceof StringNode textNode )
        {
            String uriString = textNode.asString();
            uri = UriDeserializer.deserializeUri( uriString );
        }
        // Explicit URI
        else if ( node.has( "uri" ) )
        {
            String uriString = node.get( "uri" )
                                   .asString();
            uri = UriDeserializer.deserializeUri( uriString );
        }

        // Explicit groups
        if ( node.has( "groups" ) )
        {
            JsonNode groupNode = node.get( "groups" );
            int count = groupNode.size();
            for ( int i = 0; i < count; i++ )
            {
                JsonNode nextNode = groupNode.get( i );
                FeatureServiceGroup nextGroup = objectReader.readValue( nextNode );
                groups.add( nextGroup );
            }
        }
        // Singleton group
        else if ( node.has( "group" ) )
        {
            FeatureServiceGroup singleton = objectReader.readValue( node );
            groups.add( singleton );
        }
        // No groups

        return new FeatureService( uri, Collections.unmodifiableSet( groups ) );
    }
}

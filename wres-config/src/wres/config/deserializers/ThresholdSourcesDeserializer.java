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
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.StringNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
import wres.config.components.ThresholdSource;
import wres.config.components.ThresholdSourceBuilder;

/**
 * Custom deserializer for a set of {@link ThresholdSource}.
 *
 * @author James Brown
 */
public class ThresholdSourcesDeserializer extends ValueDeserializer<Set<ThresholdSource>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdSourcesDeserializer.class );

    @Override
    public Set<ThresholdSource> deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
        JsonNode node = reader.readTree( jp );

        // Preserve insertion order
        Set<ThresholdSource> thresholdSources = new LinkedHashSet<>();

        // Array?
        if ( node instanceof ArrayNode array )
        {
            int nodes = array.size();
            for ( int i = 0; i < nodes; i++ )
            {
                JsonNode nextNode = array.get( i );
                if ( nextNode instanceof StringNode text )
                {
                    ThresholdSource source = this.getPlainSource( text );
                    thresholdSources.add( source );
                }
                else
                {
                    ThresholdSource source = this.getElaboratedSource( mapper, nextNode );
                    thresholdSources.add( source );
                }
            }
        }
        else if ( node instanceof StringNode text )
        {
            ThresholdSource source = this.getPlainSource( text );
            thresholdSources.add( source );
        }
        // Single threshold
        else
        {
            ThresholdSource source = this.getElaboratedSource( mapper, node );
            thresholdSources.add( source );
        }

        return Collections.unmodifiableSet( thresholdSources );
    }

    /**
     * Creates a threshold source from a plain URI node.
     * @param plainNode the plain node
     * @return the threshold source
     */

    private ThresholdSource getPlainSource( StringNode plainNode )
    {
        String uriString = plainNode.stringValue();
        LOGGER.debug( "Encountered a simple threshold source containing a URI: {}", uriString );
        URI uri = UriDeserializer.deserializeUri( uriString );
        return ThresholdSourceBuilder.builder()
                                     .uri( uri )
                                     .build();
    }

    /**
     * Creates a threshold source from an elaborated node.
     * @param mapper the mapper
     * @param elaboratedNode the elabrated node
     * @return the source
     */
    private ThresholdSource getElaboratedSource( ObjectMapper mapper, JsonNode elaboratedNode )
    {
        LOGGER.debug( "Encountered an elaborated threshold source." );
        ObjectReader reader = mapper.readerFor( ThresholdSource.class );
        return reader.readValue( elaboratedNode );
    }
}


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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;

/**
 * Custom deserializer for a set of {@link ThresholdSource}.
 *
 * @author James Brown
 */
public class ThresholdSourcesDeserializer extends JsonDeserializer<Set<ThresholdSource>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdSourcesDeserializer.class );

    @Override
    public Set<ThresholdSource> deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        // Preserve insertion order
        Set<ThresholdSource> thresholdSources = new LinkedHashSet<>();

        // Array?
        if ( node instanceof ArrayNode array )
        {
            int nodes = array.size();
            for ( int i = 0; i < nodes; i++ )
            {
                JsonNode nextNode = array.get( i );
                if ( nextNode instanceof TextNode text )
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
        else if ( node instanceof TextNode text )
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

    private ThresholdSource getPlainSource( TextNode plainNode )
    {
        String uriString = plainNode.textValue();
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
     * @throws IOException if the source could not be read for any reason
     */
    private ThresholdSource getElaboratedSource( ObjectReader mapper, JsonNode elaboratedNode ) throws IOException
    {
        LOGGER.debug( "Encountered an elaborated threshold source." );
        return mapper.readValue( elaboratedNode, ThresholdSource.class );
    }
}


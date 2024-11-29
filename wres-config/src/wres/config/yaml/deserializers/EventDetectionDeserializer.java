package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.components.EventDetection;
import wres.config.yaml.components.EventDetectionBuilder;
import wres.config.yaml.components.EventDetectionDataset;

/**
 * Custom deserializer for a {@link EventDetection}.
 *
 * @author James Brown
 */
public class EventDetectionDeserializer extends JsonDeserializer<EventDetection>
{
    @Override
    public EventDetection deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        Set<EventDetectionDataset> datasets = Set.of();

        // Single string
        if ( node.isTextual() )
        {
            String datasetString = node.asText()
                                       .toUpperCase();
            EventDetectionDataset dataset = EventDetectionDataset.valueOf( datasetString );
            datasets = Collections.singleton( dataset );
        }
        else if ( node.has( "dataset" ) )
        {
            JsonNode datasetNode = node.get( "dataset" );
            if ( datasetNode.isTextual() )
            {
                String datasetString = datasetNode.asText()
                                                  .toUpperCase();
                EventDetectionDataset dataset = EventDetectionDataset.valueOf( datasetString );
                datasets = Collections.singleton( dataset );
            }
            else if ( datasetNode.isArray() )
            {
                JavaType type = reader.getTypeFactory()
                                      .constructCollectionType( Set.class, EventDetectionDataset.class );
                JsonParser parser = reader.treeAsTokens( datasetNode );
                datasets = reader.readValue( parser, type );
            }
        }

        return EventDetectionBuilder.builder()
                                    .datasets( datasets )
                                    .build();
    }

}
package wres.config.yaml.deserializers;

import java.io.IOException;
import java.time.Duration;
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
import wres.config.yaml.components.EventDetectionMethod;
import wres.config.yaml.components.EventDetectionParametersBuilder;

/**
 * Custom deserializer for a {@link EventDetection}.
 *
 * @author James Brown
 */
public class EventDetectionDeserializer extends JsonDeserializer<EventDetection>
{

    /** Duration unit name. */
    private static final String DURATION_UNIT = "duration_unit";

    @Override
    public EventDetection deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        Set<EventDetectionDataset> datasets = Set.of();

        EventDetectionMethod method = null;

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

        if ( node.has( "method" ) )
        {
            JsonNode methodNode = node.get( "method" );
            method = reader.readValue( methodNode, EventDetectionMethod.class );
        }

        EventDetectionParametersBuilder parameters = EventDetectionParametersBuilder.builder();

        if ( node.has( "parameters" ) )
        {
            JsonNode parametersNode = node.get( "parameters" );
            if ( parametersNode.has( "window_size" ) )
            {
                Duration windowSize = DurationDeserializer.getDuration( reader,
                                                                        parametersNode,
                                                                        "window_size",
                                                                        DURATION_UNIT );
                parameters.windowSize( windowSize );
            }
            if ( parametersNode.has( "start_radius" ) )
            {
                Duration windowSize = DurationDeserializer.getDuration( reader,
                                                                        parametersNode,
                                                                        "start_radius",
                                                                        DURATION_UNIT );
                parameters.windowSize( windowSize );
            }
            if ( parametersNode.has( "half_life" ) )
            {
                Duration windowSize = DurationDeserializer.getDuration( reader,
                                                                        parametersNode,
                                                                        "half_life",
                                                                        DURATION_UNIT );
                parameters.windowSize( windowSize );
            }
            if ( parametersNode.has( "minimum_event_duration" ) )
            {
                Duration windowSize = DurationDeserializer.getDuration( reader,
                                                                        parametersNode,
                                                                        "minimum_event_duration",
                                                                        DURATION_UNIT );
                parameters.windowSize( windowSize );
            }
        }
        return EventDetectionBuilder.builder()
                                    .datasets( datasets )
                                    .method( method )
                                    .parameters( parameters.build() )
                                    .build();
    }
}
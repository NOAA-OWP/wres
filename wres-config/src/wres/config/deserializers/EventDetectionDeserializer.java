package wres.config.deserializers;

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

import wres.config.components.EventDetection;
import wres.config.components.TimeWindowAggregation;
import wres.config.components.EventDetectionBuilder;
import wres.config.components.EventDetectionCombination;
import wres.config.components.EventDetectionDataset;
import wres.config.components.EventDetectionMethod;
import wres.config.components.EventDetectionParameters;
import wres.config.components.EventDetectionParametersBuilder;

/**
 * Custom deserializer for a {@link EventDetection}.
 *
 * @author James Brown
 */
public class EventDetectionDeserializer extends JsonDeserializer<EventDetection>
{

    /** Duration unit name. */
    private static final String DURATION_UNIT = "duration_unit";
    private static final String MINIMUM_EVENT_DURATION = "minimum_event_duration";
    private static final String HALF_LIFE = "half_life";
    private static final String START_RADIUS = "start_radius";
    private static final String WINDOW_SIZE = "window_size";

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

        EventDetectionParameters parameters = this.deserializeParameters( node, reader );

        return EventDetectionBuilder.builder()
                                    .datasets( datasets )
                                    .method( method )
                                    .parameters( parameters )
                                    .build();
    }

    /**
     * Deserializes the event detection parameters.
     * @param node the node
     * @param reader the reader
     * @return the parameters
     * @throws IOException if the parameters could not be deserialized for any reason
     */
    private EventDetectionParameters deserializeParameters( JsonNode node, ObjectReader reader ) throws IOException
    {
        EventDetectionParametersBuilder parameters = EventDetectionParametersBuilder.builder();

        if ( node.has( "parameters" ) )
        {
            JsonNode parametersNode = node.get( "parameters" );
            if ( parametersNode.has( WINDOW_SIZE ) )
            {
                this.validateDurationUnit( parametersNode, WINDOW_SIZE );
                Duration windowSize = DurationDeserializer.getDuration( reader,
                                                                        parametersNode,
                                                                        WINDOW_SIZE,
                                                                        DURATION_UNIT );
                parameters.windowSize( windowSize );
            }
            if ( parametersNode.has( START_RADIUS ) )
            {
                this.validateDurationUnit( parametersNode, START_RADIUS );
                Duration startRadius = DurationDeserializer.getDuration( reader,
                                                                         parametersNode,
                                                                         START_RADIUS,
                                                                         DURATION_UNIT );
                parameters.startRadius( startRadius );
            }
            if ( parametersNode.has( HALF_LIFE ) )
            {
                this.validateDurationUnit( parametersNode, HALF_LIFE );
                Duration halfLife = DurationDeserializer.getDuration( reader,
                                                                      parametersNode,
                                                                      HALF_LIFE,
                                                                      DURATION_UNIT );
                parameters.halfLife( halfLife );
            }
            if ( parametersNode.has( MINIMUM_EVENT_DURATION ) )
            {
                this.validateDurationUnit( parametersNode, MINIMUM_EVENT_DURATION );
                Duration minimumEventDuration = DurationDeserializer.getDuration( reader,
                                                                                  parametersNode,
                                                                                  MINIMUM_EVENT_DURATION,
                                                                                  DURATION_UNIT );
                parameters.minimumEventDuration( minimumEventDuration );
            }
            if ( parametersNode.has( "combination" ) )
            {
                JsonNode combinationNode = parametersNode.get( "combination" );
                this.deserializeCombinationParameters( combinationNode,
                                                       reader,
                                                       parameters );
            }
        }

        return parameters.build();
    }

    /**
     * Deserializes the event combination parameters.
     * @param combinationNode the combination node
     * @param reader the reader
     * @throws IOException if the parameters could not be deserialized for any reason
     */
    private void deserializeCombinationParameters( JsonNode combinationNode,
                                                   ObjectReader reader,
                                                   EventDetectionParametersBuilder parameters ) throws IOException
    {
        if ( combinationNode.isTextual() )
        {
            EventDetectionCombination operation = reader.readValue( combinationNode,
                                                                    EventDetectionCombination.class );
            parameters.combination( operation );
        }
        else
        {
            if ( combinationNode.has( "operation" ) )
            {
                JsonNode operationNode = combinationNode.get( "operation" );
                EventDetectionCombination operation = reader.readValue( operationNode,
                                                                        EventDetectionCombination.class );
                parameters.combination( operation );
            }
            if ( combinationNode.has( "aggregation" ) )
            {
                JsonNode aggregationNode = combinationNode.get( "aggregation" );
                TimeWindowAggregation aggregation = reader.readValue( aggregationNode,
                                                                      TimeWindowAggregation.class );
                parameters.aggregation( aggregation );
            }
        }
    }

    /**
     * Checks that a duration unit is declared when required.
     *
     * @param node the node
     * @param parameterName the parameter name
     * @throws IOException if the duration unit is missing
     */
    private void validateDurationUnit( JsonNode node, String parameterName ) throws IOException
    {
        if ( !node.has( EventDetectionDeserializer.DURATION_UNIT ) )
        {
            throw new IOException( "While deserializing the event detection parameters, discovered a duration "
                                   + "parameter, '"
                                   + parameterName
                                   + "', without the required duration unit, '"
                                   + EventDetectionDeserializer.DURATION_UNIT + "'. Please declare the '"
                                   + EventDetectionDeserializer.DURATION_UNIT + "' "
                                   + "and try again." );
        }
    }
}
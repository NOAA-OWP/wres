package wres.config.deserializers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;

import wres.config.DeclarationFactory;
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
public class EventDetectionDeserializer extends ValueDeserializer<EventDetection>
{

    /** Duration unit name. */
    private static final String DURATION_UNIT = "duration_unit";
    private static final String MINIMUM_EVENT_DURATION = "minimum_event_duration";
    private static final String HALF_LIFE = "half_life";
    private static final String START_RADIUS = "start_radius";
    private static final String WINDOW_SIZE = "window_size";

    @Override
    public EventDetection deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();

        Set<EventDetectionDataset> datasets = Set.of();

        EventDetectionMethod method = null;

        // Single string
        if ( node.isString() )
        {
            String datasetString = node.asString()
                                       .toUpperCase();
            EventDetectionDataset dataset = EventDetectionDataset.valueOf( datasetString );
            datasets = Collections.singleton( dataset );
        }
        else if ( node.has( "dataset" ) )
        {
            JsonNode datasetNode = node.get( "dataset" );
            if ( datasetNode.isString() )
            {
                String datasetString = datasetNode.asString()
                                                  .toUpperCase();
                EventDetectionDataset dataset = EventDetectionDataset.valueOf( datasetString );
                datasets = Collections.singleton( dataset );
            }
            else if ( datasetNode.isArray() )
            {
                JsonParser parser = mapper.treeAsTokens( datasetNode );
                JavaType type = mapper.getTypeFactory()
                                      .constructCollectionType( Set.class, EventDetectionDataset.class );
                ObjectReader objectReader = mapper.readerFor( type );
                datasets = objectReader.readValue( parser );
            }
        }

        if ( node.has( "method" ) )
        {
            JsonNode methodNode = node.get( "method" );
            ObjectReader objectReader = mapper.readerFor( EventDetectionMethod.class );
            method = objectReader.readValue( methodNode );
        }

        EventDetectionParameters parameters = this.deserializeParameters( node, mapper );

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
     */
    private EventDetectionParameters deserializeParameters( JsonNode node, ObjectMapper reader )
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
     * @param mapper the mapper
     */
    private void deserializeCombinationParameters( JsonNode combinationNode,
                                                   ObjectMapper mapper,
                                                   EventDetectionParametersBuilder parameters )
    {
        if ( combinationNode.isString() )
        {
            ObjectReader reader = mapper.readerFor( EventDetectionCombination.class );
            EventDetectionCombination operation = reader.readValue( combinationNode );
            parameters.combination( operation );
        }
        else
        {
            if ( combinationNode.has( "operation" ) )
            {
                JsonNode operationNode = combinationNode.get( "operation" );
                ObjectReader reader = mapper.readerFor( EventDetectionCombination.class );
                EventDetectionCombination operation = reader.readValue( operationNode );
                parameters.combination( operation );
            }
            if ( combinationNode.has( "aggregation" ) )
            {
                JsonNode aggregationNode = combinationNode.get( "aggregation" );
                ObjectReader reader = mapper.readerFor( TimeWindowAggregation.class );
                TimeWindowAggregation aggregation = reader.readValue( aggregationNode );
                parameters.aggregation( aggregation );
            }
        }
    }

    /**
     * Checks that a duration unit is declared when required.
     *
     * @param node the node
     * @param parameterName the parameter name
     */
    private void validateDurationUnit( JsonNode node, String parameterName )
    {
        if ( !node.has( EventDetectionDeserializer.DURATION_UNIT ) )
        {
            throw new UncheckedIOException( new IOException( "While deserializing the event detection parameters, "
                                                             + "discovered a duration parameter, '"
                                                             + parameterName
                                                             + "', without the required duration unit, '"
                                                             + EventDetectionDeserializer.DURATION_UNIT
                                                             + "'. Please declare the '"
                                                             + EventDetectionDeserializer.DURATION_UNIT + "' "
                                                             + "and try again." ) );
        }
    }
}
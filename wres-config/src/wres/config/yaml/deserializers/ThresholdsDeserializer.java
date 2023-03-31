package wres.config.yaml.deserializers;

import java.io.IOException;
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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DoubleValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdType;
import wres.statistics.generated.Threshold.ThresholdOperator;
import wres.statistics.generated.Threshold.ThresholdDataType;

/**
 * Custom deserializer for value thresholds that are contained in a simple array or embellished with attributes.
 *
 * @author James Brown
 */
public class ThresholdsDeserializer extends JsonDeserializer<Set<Threshold>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdsDeserializer.class );

    /** String re-used several times. */
    private static final String VALUE = "value";
    /** String re-used several times. */
    private static final String VALUES = "values";

    @Override
    public Set<Threshold> deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        String currentName = jp.currentName();

        LOGGER.debug( "Attempting to deserialize thresholds from the node named {}.", currentName );

        return this.deserialize( reader, node, currentName );
    }

    /**
     * Deserializes a threshold node.
     *
     * @param reader the reader, required
     * @param thresholdsNode the thresholds node, required
     * @param nodeName the name of the node, required
     * @return the thresholds
     * @throws IOException if the thresholds could not be read
     * @throws NullPointerException if any required input is null
     */

    Set<Threshold> deserialize( ObjectReader reader,
                                JsonNode thresholdsNode,
                                String nodeName ) throws IOException
    {
        Objects.requireNonNull( reader );
        Objects.requireNonNull( thresholdsNode );
        Objects.requireNonNull( nodeName );

        Set<Threshold> thresholds = new LinkedHashSet<>();

        // Determine the declaration context for the thresholds
        ThresholdType type = ThresholdType.VALUE;
        if ( "probability_thresholds" .equals( nodeName ) )
        {
            type = ThresholdType.PROBABILITY;
        }
        else if ( "classifier_thresholds" .equals( nodeName ) )
        {
            type = ThresholdType.PROBABILITY_CLASSIFIER;
        }

        // Thresholds with attributes
        if ( thresholdsNode instanceof ObjectNode )
        {
            LOGGER.debug( "Encountered an embellished set of thresholds for node {}.", nodeName );
            Set<Threshold> innerThresholds = this.getThresholds( reader, thresholdsNode, type );
            thresholds.addAll( innerThresholds );
        }
        // Array node, which is either a plain array of thresholds or an array of thresholds with attributes
        else if ( thresholdsNode instanceof ArrayNode arrayNode )
        {
            // An array of embellished thresholds
            if( arrayNode.get( 0 )
                         .has( VALUES ) )
            {
                int setCount = arrayNode.size();

                LOGGER.debug( "Encountered an array containing {} sets of embellished thresholds for node {}",
                              setCount,
                              nodeName );

                // Deserialize all sets of thresholds
                for( int i = 0; i < setCount; i++ )
                {
                    JsonNode innerThresholdsNode = arrayNode.get( i );
                    Set<Threshold> innerThresholds = this.getThresholds( reader, innerThresholdsNode, type );
                    thresholds.addAll( innerThresholds );
                }
            }
            // A plain array of thresholds
            else
            {
                LOGGER.debug( "Encountered a plain array of thresholds for node {}.", nodeName );
                wres.statistics.generated.Threshold.Builder builder
                        = wres.statistics.generated.Threshold.newBuilder()
                                                             .setOperator( ThresholdOperator.GREATER )
                                                             .setDataType( ThresholdDataType.LEFT );
                Set<Threshold> innerThresholds = this.getThresholdsFromArray( reader, arrayNode, type, builder );
                thresholds.addAll( innerThresholds );
            }
        }

        LOGGER.debug( "Deserialized the following thresholds: {}.", thresholds );

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Creates a collection of thresholds from an array node.
     * @param reader the reader
     * @param thresholdsNode the thresholds node
     * @param type the threshold type
     * @param thresholdBuilder the threshold builder
     * @return the thresholds
     * @throws IOException if the thresholds could not be mapped
     */

    private Set<Threshold> getThresholdsFromArray( ObjectReader reader,
                                                   ArrayNode thresholdsNode,
                                                   ThresholdType type,
                                                   wres.statistics.generated.Threshold.Builder thresholdBuilder )
            throws IOException
    {
        // Preserve insertion order
        Set<Threshold> thresholds = new LinkedHashSet<>();

        // Threshold values are validated at schema validation time
        double[] values = reader.readValue( thresholdsNode, double[].class );

        for ( double nextValue : values )
        {
            DoubleValue doubleValue = DoubleValue.of( nextValue );

            if ( type.isProbability() )
            {
                thresholdBuilder.setLeftThresholdProbability( doubleValue );
            }
            else
            {
                thresholdBuilder.setLeftThresholdValue( doubleValue );
            }

            wres.statistics.generated.Threshold nextThreshold = thresholdBuilder.build();
            Threshold nextWrappedThreshold = new Threshold( nextThreshold, type, null );
            thresholds.add( nextWrappedThreshold );
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Reads an embellished threshold from a node.
     * @param reader the reader
     * @param thresholdNode the threshold node
     * @param type the type of thresholds
     * @return the thresholds
     * @throws IOException if the thresholds could not be read
     */

    private Set<Threshold> getThresholds( ObjectReader reader,
                                          JsonNode thresholdNode,
                                          ThresholdType type ) throws IOException
    {
        wres.statistics.generated.Threshold.Builder builder
                = wres.statistics.generated.Threshold.newBuilder();

        if ( thresholdNode.has( "name" ) )
        {
            JsonNode nameNode = thresholdNode.get( "name" );
            String name = nameNode.asText();
            builder.setName( name );
        }

        if ( thresholdNode.has( "operator" ) )
        {
            JsonNode operatorNode = thresholdNode.get( "operator" );

            // Map between user-friendly and canonical operators
            wres.config.yaml.components.ThresholdOperator friendlyOperator =
                    reader.readValue( operatorNode, wres.config.yaml.components.ThresholdOperator.class );
            ThresholdOperator operator = friendlyOperator.canonical();
            builder.setOperator( operator );
        }

        if ( thresholdNode.has( "apply_to" ) )
        {
            JsonNode dataTypeNode = thresholdNode.get( "apply_to" );
            ThresholdDataType dataType = DeclarationFactory.getThresholdDataType( dataTypeNode.asText() );
            builder.setDataType( dataType );
        }

        if( thresholdNode.has( "unit" ) )
        {
            JsonNode unitNode = thresholdNode.get( "unit" );
            String unitString = unitNode.asText();
            builder.setThresholdValueUnits( unitString );
        }

        // Preserve insertion order
        Set<Threshold> thresholds = new LinkedHashSet<>();

        // Create the thresholds
        if ( thresholdNode.has( VALUES ) )
        {
            JsonNode valuesNode = thresholdNode.get( VALUES );

            // Embellished thresholds
            if ( valuesNode.size() > 0 && valuesNode.get( 0 )
                                                    .has( VALUE ) )
            {
                Set<Threshold> embellishedThresholds = this.getEmbellishedThresholds( valuesNode,
                                                                                      type,
                                                                                      builder );
                thresholds.addAll( embellishedThresholds );
            }
            // Plain thresholds
            else if ( valuesNode instanceof ArrayNode arrayNode )
            {
                Set<Threshold> plainThresholds = this.getThresholdsFromArray( reader,
                                                                              arrayNode,
                                                                              type,
                                                                              builder );
                thresholds.addAll( plainThresholds );
            }
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Creates a set of thresholds with attributes.
     * @param thresholdNode the threshold node
     * @param type the type of thresholds
     * @param builder the threshold builder
     * @return the thresholds
     */

    private Set<Threshold> getEmbellishedThresholds( JsonNode thresholdNode,
                                                     ThresholdType type,
                                                     wres.statistics.generated.Threshold.Builder builder )
    {
        int thresholdCount = thresholdNode.size();

        // Preserve insertion order
        Set<Threshold> thresholds = new LinkedHashSet<>();

        // Iterate the thresholds
        for ( int i = 0; i < thresholdCount; i++ )
        {
            // Clear any existing state
            builder.clearLeftThresholdValue();
            builder.clearLeftThresholdProbability();
            builder.clearRightThresholdValue();
            builder.clearRightThresholdProbability();

            JsonNode nextNode = thresholdNode.get( i );

            // Threshold value
            if ( nextNode.has( VALUE ) )
            {
                JsonNode valueNode = nextNode.get( VALUE );
                double value = valueNode.doubleValue();
                DoubleValue wrappedDouble = DoubleValue.of( value );

                if ( type.isProbability() )
                {
                    builder.setLeftThresholdProbability( wrappedDouble );
                }
                else
                {
                    builder.setLeftThresholdValue( wrappedDouble );
                }
            }

            // Feature name
            String featureName = null;
            if ( nextNode.has( "feature" ) )
            {
                JsonNode featureNode = nextNode.get( "feature" );
                featureName = featureNode.asText();
            }

            Threshold nextThreshold =
                    new Threshold( builder.build(), type, featureName );
            thresholds.add( nextThreshold );
        }

        return Collections.unmodifiableSet( thresholds );
    }

}

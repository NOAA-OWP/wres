package wres.config.yaml;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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

import wres.config.yaml.DeclarationFactory.Threshold;

import wres.statistics.generated.Threshold.ThresholdOperator;
import wres.statistics.generated.Threshold.ThresholdDataType;

/**
 * Custom deserializer for value thresholds that are contained in a simple array or embellished with attributes.
 *
 * @author James Brown
 */
class ThresholdsDeserializer extends JsonDeserializer<Set<Threshold>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdsDeserializer.class );

    /** Mapping of threshold operator strings to operator enums. Somewhat brittle, but allows for more user-friendly
     * naming and the protobuf enums cannot be annotated directly. */
    private static final Map<String, ThresholdOperator> THRESHOLD_OPERATORS =
            Map.of( "GREATER_THAN", ThresholdOperator.GREATER,
                    "GREATER_THAN_OR_EQUAL_TO", ThresholdOperator.GREATER_EQUAL,
                    "LESS_THAN", ThresholdOperator.LESS,
                    "LESS_THAN_OR_EQUAL_TO", ThresholdOperator.LESS_EQUAL,
                    "EQUAL_TO", ThresholdOperator.EQUAL );

    /** String re-used several times. */
    public static final String VALUE = "value";

    @Override
    public Set<Threshold> deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        String currentName = jp.currentName();
        boolean probabilities = "probability_thresholds".equals( currentName )
                                || "classifier_thresholds".equals( currentName );

        // Thresholds with attributes
        if ( node instanceof ObjectNode )
        {
            LOGGER.debug( "Encountered an embellished set of thresholds for node {}.", currentName );
            return this.getThresholds( reader, node, probabilities );
        }
        // Plain array of thresholds
        else if ( node instanceof ArrayNode arrayNode )
        {
            LOGGER.debug( "Encountered a plain array of thresholds for node {}.", currentName );
            wres.statistics.generated.Threshold.Builder builder
                    = wres.statistics.generated.Threshold.newBuilder()
                                                         .setOperator( ThresholdOperator.GREATER )
                                                         .setDataType( ThresholdDataType.LEFT );
            return this.getThresholdsFromArray( reader, arrayNode, probabilities, builder );
        }
        else
        {
            throw new IOException( "When reading the '" + currentName
                                   + "' declaration of 'thresholds', discovered an unrecognized data type. Please "
                                   + "fix this declaration and try again." );
        }
    }

    /**
     * Creates a collection of thresholds from an array node.
     * @param reader the mapper
     * @param thresholdsNode the thresholds node
     * @param probabilities is true if the threshold values are probabilities, false for regular values
     * @param thresholdBuilder the threshold builder
     * @return the thresholds
     * @throws IOException if the thresholds could not be mapped
     */

    private Set<Threshold> getThresholdsFromArray( ObjectReader reader,
                                                   ArrayNode thresholdsNode,
                                                   boolean probabilities,
                                                   wres.statistics.generated.Threshold.Builder thresholdBuilder )
            throws IOException
    {
        Set<Threshold> thresholds = new HashSet<>();

        // Threshold values are validated at schema validation time
        double[] values = reader.readValue( thresholdsNode, double[].class );

        for ( double nextValue : values )
        {
            DoubleValue doubleValue = DoubleValue.of( nextValue );

            if ( probabilities )
            {
                thresholdBuilder.setLeftThresholdProbability( doubleValue );
            }
            else
            {
                thresholdBuilder.setLeftThresholdValue( doubleValue );
            }

            wres.statistics.generated.Threshold nextThreshold = thresholdBuilder.build();
            Threshold nextWrappedThreshold = new Threshold( nextThreshold, null );
            thresholds.add( nextWrappedThreshold );
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Reads an embellished threshold from a node.
     * @param reader the reader
     * @param thresholdNode the threshold node
     * @param probabilities is true if the threshold values are probabilities, false for regular values
     * @return the thresholds
     * @throws IOException if the thresholds could not be read
     */

    private Set<Threshold> getThresholds( ObjectReader reader,
                                          JsonNode thresholdNode,
                                          boolean probabilities ) throws IOException
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
            String operatorString = operatorNode.asText()
                                                .replace( " ", "_" )
                                                .toUpperCase();
            ThresholdOperator operator = THRESHOLD_OPERATORS.get( operatorString );
            builder.setOperator( operator );
        }

        if ( thresholdNode.has( "apply_to" ) )
        {
            JsonNode dataTypeNode = thresholdNode.get( "apply_to" );
            String dataTypeString = dataTypeNode.asText()
                                                .replace( " ", "_" )
                                                .toUpperCase();
            ThresholdDataType dataType = ThresholdDataType.valueOf( dataTypeString );
            builder.setDataType( dataType );
        }

        Set<Threshold> thresholds = new HashSet<>();

        // Create the thresholds
        if ( thresholdNode.has( "values" ) )
        {
            JsonNode valuesNode = thresholdNode.get( "values" );

            // Embellished thresholds
            if ( valuesNode.size() > 0 && valuesNode.get( 0 )
                                                    .has( VALUE ) )
            {
                Set<Threshold> embellishedThresholds = this.getEmbellishedThresholds( valuesNode,
                                                                                      probabilities,
                                                                                      builder );
                thresholds.addAll( embellishedThresholds );
            }
            // Plain thresholds
            else if ( valuesNode instanceof ArrayNode arrayNode )
            {
                Set<Threshold> plainThresholds = this.getThresholdsFromArray( reader,
                                                                              arrayNode,
                                                                              probabilities,
                                                                              builder );
                thresholds.addAll( plainThresholds );
            }
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Creates a set of thresholds with attributes.
     * @param thresholdNode the threshold node
     * @param probabilities is true if the threshold values are probabilities, false for regular values
     * @param builder the threshold builder
     * @return the thresholds
     */

    private Set<Threshold> getEmbellishedThresholds( JsonNode thresholdNode,
                                                     boolean probabilities,
                                                     wres.statistics.generated.Threshold.Builder builder )
    {
        int thresholdCount = thresholdNode.size();
        Set<Threshold> thresholds = new HashSet<>();

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

                if ( probabilities )
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

            Threshold nextThreshold = new Threshold( builder.build(), featureName );
            thresholds.add( nextThreshold );
        }

        return Collections.unmodifiableSet( thresholds );
    }

}

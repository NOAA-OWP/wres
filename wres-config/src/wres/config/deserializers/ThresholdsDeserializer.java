package wres.config.deserializers;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
import wres.config.DeclarationUtilities;
import wres.config.components.DatasetOrientation;
import wres.config.components.Threshold;
import wres.config.components.ThresholdBuilder;
import wres.config.components.ThresholdOrientation;
import wres.config.components.ThresholdType;
import wres.statistics.generated.Geometry;
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
        ThresholdType type = DeclarationUtilities.getThresholdType( nodeName );

        // Thresholds with attributes
        if ( thresholdsNode instanceof ObjectNode )
        {
            LOGGER.debug( "Encountered an embellished set of thresholds for node {}.", nodeName );
            Set<Threshold> innerThresholds = this.getThresholds( reader, thresholdsNode, type );
            thresholds.addAll( innerThresholds );
        }
        // Schema requires a constant value, all data
        else if ( thresholdsNode.isTextual() )
        {
            LOGGER.debug( "Discovered an 'all data' threshold." );
            thresholds.add( DeclarationUtilities.ALL_DATA_THRESHOLD );
        }
        // Implicit array of one
        else if ( thresholdsNode.isNumber() )
        {
            LOGGER.debug( "Encountered a plain array of thresholds for node {}.", nodeName );
            Set<Threshold> innerThresholds = this.getSingletonThreshold( thresholdsNode, type, null );
            thresholds.addAll( innerThresholds );
        }
        // Explicit array node, which is either a plain array of thresholds or an array of thresholds with attributes
        else if ( thresholdsNode instanceof ArrayNode arrayNode )
        {
            // Sentinel for all data
            if ( arrayNode.isEmpty() )
            {
                LOGGER.debug( "Discovered an empty threshold array, adding an 'all data' threshold only." );
                thresholds.add( DeclarationUtilities.ALL_DATA_THRESHOLD );
            }
            // An array of embellished thresholds
            else if ( arrayNode.get( 0 )
                               .has( VALUES ) )
            {
                int setCount = arrayNode.size();

                LOGGER.debug( "Encountered an array containing {} sets of embellished thresholds for node {}",
                              setCount,
                              nodeName );

                // Deserialize all sets of thresholds
                for ( int i = 0; i < setCount; i++ )
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
                        = DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder();
                Set<Threshold> innerThresholds = this.getThresholdsFromArray( reader, arrayNode, type, builder );
                thresholds.addAll( innerThresholds );
            }
        }
        else
        {
            throw new IOException( "Unsupported type of threshold node: " + thresholdsNode.getClass() );
        }

        LOGGER.debug( "Deserialized the following thresholds: {}.", thresholds );

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Creates a singleton threshold from an implicit array.
     * @param thresholdNode the threshold node
     * @param type the threshold type
     * @param builder an optional builder
     * @return the thresholds
     */

    private Set<Threshold> getSingletonThreshold( JsonNode thresholdNode,
                                                  ThresholdType type,
                                                  wres.statistics.generated.Threshold.Builder builder )
    {
        // No builder supplied?
        if ( Objects.isNull( builder ) )
        {
            builder = DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder();
        }

        double lowerValue = thresholdNode.asDouble();

        if ( builder.getOperator() == ThresholdOperator.BETWEEN )
        {
            if ( type.isProbability() )
            {
                builder.setPredictedThresholdProbability( 1.0 );
            }
            else
            {
                builder.setPredictedThresholdValue( Double.POSITIVE_INFINITY );
            }
        }

        if ( type.isProbability() )
        {
            builder.setObservedThresholdProbability( lowerValue );
        }
        else
        {
            builder.setObservedThresholdValue( lowerValue );
        }

        wres.statistics.generated.Threshold nextThreshold = builder.build();
        Threshold wrappedThreshold = ThresholdBuilder.builder()
                                                     .threshold( nextThreshold )
                                                     .type( type )
                                                     .build();

        return Set.of( wrappedThreshold );
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
        wres.statistics.generated.Threshold.Builder builder =
                DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder();

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
            wres.config.components.ThresholdOperator friendlyOperator =
                    reader.readValue( operatorNode, wres.config.components.ThresholdOperator.class );
            ThresholdOperator operator = friendlyOperator.canonical();
            builder.setOperator( operator );
        }

        if ( thresholdNode.has( "apply_to" ) )
        {
            JsonNode dataTypeNode = thresholdNode.get( "apply_to" );
            ThresholdOrientation orientation = reader.readValue( dataTypeNode, ThresholdOrientation.class );
            ThresholdDataType dataType = orientation.canonical();
            builder.setDataType( dataType );
        }

        if ( thresholdNode.has( "unit" ) )
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
            if ( !valuesNode.isEmpty()
                 && valuesNode.get( 0 )
                              .has( VALUE ) )
            {
                Set<Threshold> embellishedThresholds = this.getEmbellishedThresholds( reader,
                                                                                      valuesNode,
                                                                                      thresholdNode,
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
            // Singleton
            else if ( valuesNode.isNumber() )
            {
                LOGGER.debug( "Encountered a threshold value node with a singleton threshold." );
                Set<Threshold> innerThresholds = this.getSingletonThreshold( valuesNode, type, builder );
                thresholds.addAll( innerThresholds );
            }
            else
            {
                throw new IOException( "Unsupported type of threshold values node: " + valuesNode.getClass() );
            }
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Creates a set of thresholds with attributes.
     * @param reader the reader
     * @param valuesNode the threshold node
     * @param thresholdNode the parent of the value node from which the feature orientation may be obtained
     * @param type the type of thresholds
     * @param builder the threshold builder
     * @return the thresholds
     * @throws IOException if the embellished attributes could not be read
     */

    private Set<Threshold> getEmbellishedThresholds( ObjectReader reader,
                                                     JsonNode valuesNode,
                                                     JsonNode thresholdNode,
                                                     ThresholdType type,
                                                     wres.statistics.generated.Threshold.Builder builder )
            throws IOException
    {
        int thresholdCount = valuesNode.size();

        // Preserve insertion order
        Set<Threshold> thresholds = new LinkedHashSet<>();

        // Iterate the thresholds
        for ( int i = 0; i < thresholdCount; i++ )
        {
            // Clear any existing state
            builder.clearObservedThresholdValue();
            builder.clearObservedThresholdProbability();
            builder.clearPredictedThresholdValue();
            builder.clearPredictedThresholdProbability();

            JsonNode nextNode = valuesNode.get( i );

            // Threshold value. Set all as Observed. Any BETWEEN thresholds are constructed by merging later
            if ( nextNode.has( VALUE ) )
            {
                JsonNode valueNode = nextNode.get( VALUE );
                double value = valueNode.doubleValue();
                if ( type.isProbability() )
                {
                    builder.setObservedThresholdProbability( value );
                }
                else
                {
                    builder.setObservedThresholdValue( value );
                }
            }

            // Feature name
            Geometry feature = null;
            if ( nextNode.has( "feature" ) )
            {
                JsonNode featureNode = nextNode.get( "feature" );
                String featureName = featureNode.asText();
                feature = Geometry.newBuilder()
                                  .setName( featureName )
                                  .build();
            }

            DatasetOrientation featureNameFrom = null;

            if ( thresholdNode.has( "feature_name_from" ) )
            {
                JsonNode orientationNode = thresholdNode.get( "feature_name_from" );
                featureNameFrom = reader.readValue( orientationNode, DatasetOrientation.class );
            }

            Threshold nextThreshold =
                    ThresholdBuilder.builder()
                                    .threshold( builder.build() )
                                    .type( type )
                                    .feature( feature )
                                    .featureNameFrom( featureNameFrom )
                                    .build();
            thresholds.add( nextThreshold );
        }

        return DeclarationUtilities.mergeBetweenThresholds( Collections.unmodifiableSet( thresholds ) );
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

        for ( int i = 0; i < values.length; i++ )
        {
            double lowerValue = values[i];

            // Between operator, bounded to positive infinity if there is one threshold, otherwise in pairs
            if ( thresholdBuilder.getOperator() == ThresholdOperator.BETWEEN )
            {
                // Set the default
                double upperValue = this.getDefaultUpperLimit( type );

                if ( i > 0 )
                {
                    lowerValue = values[i - 1];
                    upperValue = values[i];
                }
                else if ( values.length > 1 )  // && i==0
                {
                    // Skip this value
                    continue;
                }

                if ( type.isProbability() )
                {
                    thresholdBuilder.setPredictedThresholdProbability( upperValue );
                }
                else
                {
                    thresholdBuilder.setPredictedThresholdValue( upperValue );
                }
            }

            if ( type.isProbability() )
            {
                thresholdBuilder.setObservedThresholdProbability( lowerValue );
            }
            else
            {
                thresholdBuilder.setObservedThresholdValue( lowerValue );
            }

            wres.statistics.generated.Threshold nextThreshold = thresholdBuilder.build();
            Threshold nextWrappedThreshold = ThresholdBuilder.builder()
                                                             .threshold( nextThreshold )
                                                             .type( type )
                                                             .build();
            thresholds.add( nextWrappedThreshold );
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Returns the upper limit on a threshold.
     * @param type the type of threshold
     * @return the upper limit
     */

    private double getDefaultUpperLimit( ThresholdType type )
    {
        if ( type.isProbability() )
        {
            return 1.0;
        }

        return Double.POSITIVE_INFINITY;
    }

}

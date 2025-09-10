package wres.config.serializers;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
import wres.config.DeclarationUtilities;
import wres.config.components.Threshold;
import wres.config.components.ThresholdBuilder;
import wres.config.components.ThresholdOrientation;
import wres.config.components.ThresholdType;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Threshold.ThresholdDataType;
import wres.statistics.generated.Threshold.ThresholdOperator;

/**
 * Serializes a set of {@link Threshold}. The thresholds serialized by this class should all have the same threshold
 * metadata, other than feature names.
 * @author James Brown
 */
public class ThresholdsSerializer extends JsonSerializer<Set<Threshold>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdsSerializer.class );

    /** Re-used string. */
    private static final String VALUE = "value";

    @Override
    public void serialize( Set<Threshold> thresholds,
                           JsonGenerator writer,
                           SerializerProvider serializers ) throws IOException
    {
        // Determine the type of thresholds
        String context = writer.getOutputContext()
                               .getCurrentName();

        ThresholdType type = DeclarationUtilities.getThresholdType( context );

        if ( !thresholds.isEmpty() )
        {
            LOGGER.debug( "Discovered a collection of thresholds with {} members.", thresholds.size() );

            // Write a simple array
            if ( this.hasDefaultMetadata( thresholds ) )
            {
                double[] values = this.getThresholdValues( type, thresholds );
                writer.writeObject( values );
            }
            // Write the thresholds with their full metadata
            else
            {
                this.groupAndWriteThresholdsWithFullMetadata( thresholds, type, writer );
            }
        }
    }

    @Override
    public boolean isEmpty( SerializerProvider provider, Set<Threshold> thresholds )
    {
        return thresholds.isEmpty();
    }

    /**
     * Groups the thresholds by common name and writes the thresholds with complete metadata.
     * @param thresholds the thresholds
     * @param type the threshold type
     * @param writer the writer
     * @throws IOException if the threshold could not be written for any reason
     */

    private void groupAndWriteThresholdsWithFullMetadata( Set<Threshold> thresholds,
                                                          ThresholdType type,
                                                          JsonGenerator writer ) throws IOException
    {
        // Preserve insertion order
        List<Set<Threshold>> grouped = this.getGroupedThresholds( thresholds );

        // Write the groups to an array, one set in each position
        if ( grouped.size() > 1 )
        {
            writer.writeStartArray();
            for ( Set<Threshold> nextThresholds : grouped )
            {
                this.writeThresholdsWithFullMetadata( nextThresholds, type, writer );
            }
            writer.writeEndArray();
        }
        // Write the single set to an object
        else
        {
            this.writeThresholdsWithFullMetadata( thresholds, type, writer );
        }
    }

    /**
     * Writes the thresholds with complete metadata.
     * @param thresholds the thresholds
     * @param type the threshold type
     * @param writer the writer
     * @throws IOException if the threshold could not be written for any reason
     */

    private void writeThresholdsWithFullMetadata( Set<Threshold> thresholds,
                                                  ThresholdType type,
                                                  JsonGenerator writer ) throws IOException
    {
        writer.writeStartObject();

        // For the basic metadata
        Threshold first = thresholds.iterator()
                                    .next();

        wres.statistics.generated.Threshold innerThreshold = first.threshold();

        if ( !innerThreshold.getName()
                            .isBlank() )
        {
            writer.writeStringField( "name", innerThreshold.getName() );
        }

        // More than one feature?
        if ( thresholds.stream()
                       .anyMatch( next -> Objects.nonNull( next.feature() ) ) )
        {
            this.writeFeaturefulThresholdValues( thresholds, type, writer );
        }
        // One set of thresholds, print a simple array
        else
        {
            double[] values = this.getThresholdValues( type, thresholds );
            writer.writeObjectField( "values", values );
        }

        // Write the user-friendly threshold operator name, if not default
        if ( innerThreshold.getOperator() != DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.getOperator() )
        {
            ThresholdOperator operator = innerThreshold.getOperator();
            String operatorName = DeclarationUtilities.fromEnumName( operator.name() );
            writer.writeStringField( "operator", operatorName );
        }
        // Write the data orientation
        if ( innerThreshold.getDataType() != DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.getDataType() )
        {
            ThresholdDataType dataType = innerThreshold.getDataType();
            ThresholdOrientation orientation = ThresholdOrientation.valueOf( dataType.name() );
            writer.writeObjectField( "apply_to", orientation );
        }

        if ( type == ThresholdType.VALUE
             && !innerThreshold.getThresholdValueUnits()
                               .isBlank() )
        {
            writer.writeStringField( "unit", innerThreshold.getThresholdValueUnits() );
        }

        if ( Objects.nonNull( first.featureNameFrom() )
             && first.featureNameFrom() != Threshold.DEFAULT_FEATURE_NAME_FROM )
        {
            writer.writeStringField( "feature_name_from", first.featureNameFrom()
                                                               .toString() );
        }

        writer.writeEndObject();
    }

    /**
     * Writes the threshold values when one or more are featureful.
     * @param thresholds the featureful thresholds
     * @param type the threshold type
     * @param writer the writer
     * @throws IOException if the writing fails for any reason
     */

    private void writeFeaturefulThresholdValues( Set<Threshold> thresholds,
                                                 ThresholdType type,
                                                 JsonGenerator writer ) throws IOException
    {
        writer.writeFieldName( "values" );

        // Start the array
        writer.writeStartArray();

        // Get the featureful thresholds and write those
        Set<Threshold> featureful = thresholds.stream()
                                              .filter( next -> Objects.nonNull( next.feature() ) )
                                              .collect( Collectors.toCollection( LinkedHashSet::new ) );
        // Preserve insertion order
        Map<Geometry, List<Threshold>> groupedByFeature
                = featureful.stream()
                            .collect( Collectors.groupingBy( Threshold::feature,
                                                             LinkedHashMap::new,
                                                             Collectors.toList() ) );

        for ( Map.Entry<Geometry, List<Threshold>> nextGroup : groupedByFeature.entrySet() )
        {
            Geometry geometry = nextGroup.getKey();
            List<Threshold> nextThresholds = nextGroup.getValue();
            double[] values = this.getThresholdValues( type, nextThresholds );
            for ( double nextValue : values )
            {
                // Use flow style if possible
                if ( writer instanceof CustomGenerator custom )
                {
                    custom.setFlowStyleOn();
                }

                writer.writeStartObject();
                writer.writeNumberField( VALUE, nextValue );
                writer.writeStringField( "feature", geometry.getName() );
                writer.writeEndObject();

                // Return to default style
                if ( writer instanceof CustomGenerator custom )
                {
                    custom.setFlowStyleOff();
                }
            }
        }

        // Get the featureless thresholds and write those
        Set<Threshold> featureless = thresholds.stream()
                                               .filter( next -> Objects.isNull( next.feature() ) )
                                               .collect( Collectors.toCollection( LinkedHashSet::new ) );
        double[] values = this.getThresholdValues( type, featureless );
        for ( double nextValue : values )
        {
            writer.writeStartObject();
            writer.writeNumberField( VALUE, nextValue );
            writer.writeEndObject();
        }

        // End the array
        writer.writeEndArray();
    }

    /**
     * Groups the thresholds for serialization.
     * @param thresholds the thresholds to group
     * @return the grouped thresholds
     */

    private List<Set<Threshold>> getGroupedThresholds( Set<Threshold> thresholds )
    {
        // Group by every attribute of the threshold except the values, retaining insertion order
        Map<ThresholdAttributes, Set<Threshold>> grouped
                = thresholds.stream()
                            .collect( Collectors.groupingBy( t -> new ThresholdAttributes( t.threshold()
                                                                                            .getOperator(),
                                                                                           t.threshold()
                                                                                            .getDataType(),
                                                                                           t.threshold()
                                                                                            .getName() ),
                                                             LinkedHashMap::new,
                                                             Collectors.toCollection( LinkedHashSet::new ) ) );

        return grouped.values()
                      .stream()
                      .toList();
    }

    /**
     * Bag of threshold attributes for ordering.
     * @param operator the operator
     * @param dataType the data type
     * @param name the name
     */
    private record ThresholdAttributes( ThresholdOperator operator,
                                        ThresholdDataType dataType,
                                        String name )
    {
    }

    /**
     * @param type the threshold type
     * @param thresholds the thresholds
     * @return the threshold values
     */

    private double[] getThresholdValues( ThresholdType type, Collection<Threshold> thresholds )
    {
        double[] values;
        if ( type == ThresholdType.VALUE )
        {
            values = thresholds.stream()
                               .mapToDouble( next -> next.threshold()
                                                         .getObservedThresholdValue() )
                               .sorted()
                               .toArray();
        }
        else
        {
            values = thresholds.stream()
                               .mapToDouble( next -> next.threshold()
                                                         .getObservedThresholdProbability() )
                               .sorted()
                               .toArray();
        }

        return values;
    }

    /**
     * @param thresholds the thresholds
     * @return whether the thresholds have default metadata
     */

    private boolean hasDefaultMetadata( Set<Threshold> thresholds )
    {
        for ( Threshold next : thresholds )
        {
            wres.statistics.generated.Threshold blank = next.threshold()
                                                            .toBuilder()
                                                            .clearObservedThresholdProbability()
                                                            .clearPredictedThresholdProbability()
                                                            .clearObservedThresholdValue()
                                                            .clearPredictedThresholdValue()
                                                            .build();

            Threshold blankOuter = ThresholdBuilder.builder()
                                                   .threshold( blank )
                                                   .build();

            if ( !blankOuter.equals( DeclarationFactory.DEFAULT_THRESHOLD )
                 || Objects.nonNull( next.feature() ) )
            {
                return false;
            }
        }

        return true;
    }

}

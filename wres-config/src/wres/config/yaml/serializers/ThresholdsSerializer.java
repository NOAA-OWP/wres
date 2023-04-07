package wres.config.yaml.serializers;

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

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdBuilder;
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

    @Override
    public void serialize( Set<Threshold> thresholds,
                           JsonGenerator writer,
                           SerializerProvider serializers ) throws IOException
    {
        // Determine the type of thresholds
        String context = writer.getOutputContext()
                               .getCurrentName();

        if ( !thresholds.isEmpty() )
        {
            LOGGER.debug( "Discovered a collection of thresholds with {} members.", thresholds.size() );

            // Write a simple array
            if ( this.hasDefaultMetadata( thresholds ) )
            {
                double[] values = this.getThresholdValues( context, thresholds );
                writer.writeObject( values );
            }
            // Write the thresholds with their full metadata
            else
            {
                this.groupAndWriteThresholdsWithFullMetadata( thresholds, context, writer );
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
     * @param context the context
     * @param writer the writer
     * @throws IOException if the threshold could not be written for any reason
     */

    private void groupAndWriteThresholdsWithFullMetadata( Set<Threshold> thresholds,
                                                          String context,
                                                          JsonGenerator writer ) throws IOException
    {


        // Preserve insertion order
        Map<String, Set<Threshold>> grouped
                = thresholds.stream()
                            .collect( Collectors.groupingBy( next -> next.threshold()
                                                                         .getName(),
                                                             LinkedHashMap::new,
                                                             Collectors.toCollection( LinkedHashSet::new ) ) );
        // Write the groups to an array, one set in each position
        if ( grouped.size() > 1 )
        {
            writer.writeStartArray();
            for ( Set<Threshold> nextThresholds : grouped.values() )
            {
                this.writeThresholdsWithFullMetadata( nextThresholds, context, writer );
            }
            writer.writeEndArray();
        }
        // Write the single set to an object
        else
        {
            this.writeThresholdsWithFullMetadata( thresholds, context, writer );
        }
    }

    /**
     * Writes the thresholds with complete metadata.
     * @param thresholds the thresholds
     * @param context the context
     * @param writer the writer
     * @throws IOException if the threshold could not be written for any reason
     */

    private void writeThresholdsWithFullMetadata( Set<Threshold> thresholds,
                                                  String context,
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
            writer.writeFieldName( "values" );

            // Start the array
            writer.writeStartArray();

            // Preserve insertion order
            Map<Geometry, List<Threshold>> groupedByFeature
                    = thresholds.stream()
                                .collect( Collectors.groupingBy( Threshold::feature,
                                                                 LinkedHashMap::new,
                                                                 Collectors.toList() ) );

            for ( Map.Entry<Geometry, List<Threshold>> nextGroup : groupedByFeature.entrySet() )
            {
                Geometry geometry = nextGroup.getKey();
                List<Threshold> nextThresholds = nextGroup.getValue();
                double[] values = this.getThresholdValues( context, nextThresholds );
                for ( double nextValue : values )
                {
                    writer.writeStartObject();
                    writer.writeNumberField( "value", nextValue );
                    writer.writeStringField( "feature", geometry.getName() );
                    writer.writeEndObject();
                }
            }

            // End the array
            writer.writeEndArray();
        }
        // One set of thresholds, print a simple array
        else
        {
            double[] values = this.getThresholdValues( context, thresholds );
            writer.writeObjectField( "values", values );
        }

        // Write the user-friendly threshold operator name, if not default
        if ( innerThreshold.getOperator() != DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.getOperator() )
        {
            ThresholdOperator operator = innerThreshold.getOperator();
            String operatorName = DeclarationFactory.fromEnumName( operator.name() );
            writer.writeStringField( "operator", operatorName );
        }
        // Write the data orientation
        if ( innerThreshold.getDataType() != DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.getDataType() )
        {
            ThresholdDataType dataType = innerThreshold.getDataType();
            String sideName = DeclarationFactory.getThresholdDataTypeName( dataType );
            writer.writeStringField( "apply_to", sideName );
        }

        if ( !innerThreshold.getThresholdValueUnits()
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
     * @param context the context
     * @param thresholds the thresholds
     * @return the threshold values
     */

    private double[] getThresholdValues( String context, Collection<Threshold> thresholds )
    {
        double[] values;
        if ( context.startsWith( "value" ) )
        {
            values = thresholds.stream()
                               .mapToDouble( next -> next.threshold()
                                                         .getLeftThresholdValue()
                                                         .getValue() )
                               .sorted()
                               .toArray();
        }
        else
        {
            values = thresholds.stream()
                               .mapToDouble( next -> next.threshold()
                                                         .getLeftThresholdProbability()
                                                         .getValue() )
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
                                                            .clearLeftThresholdProbability()
                                                            .clearRightThresholdProbability()
                                                            .clearLeftThresholdValue()
                                                            .clearRightThresholdValue()
                                                            .build();

            Threshold blankOuter = new Threshold( blank, null, null, null );

            if ( !blankOuter.equals( DeclarationFactory.DEFAULT_THRESHOLD )
                 || Objects.nonNull( next.feature() ) )
            {
                return false;
            }
        }

        return true;
    }

}

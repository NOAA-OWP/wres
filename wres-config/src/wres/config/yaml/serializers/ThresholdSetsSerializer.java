package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdType;

/**
 * Serializes a set of {@link Threshold}. The thresholds serialized by this class are decomposed into groups with
 * common threshold metadata.
 * @author James Brown
 */
public class ThresholdSetsSerializer extends JsonSerializer<Set<Threshold>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdSetsSerializer.class );

    /** Thresholds serializer. */
    private static final ThresholdsSerializer THRESHOLDS_SERIALIZER = new ThresholdsSerializer();

    @Override
    public void serialize( Set<Threshold> thresholds,
                           JsonGenerator writer,
                           SerializerProvider serializers ) throws IOException
    {
        Map<Threshold, Set<Threshold>> grouped = this.groupThresholdsByType( thresholds );

        LOGGER.debug( "Discovered threshold sets with {} members.", grouped.size() );

        if( !grouped.isEmpty() )
        {
            writer.writeStartObject();
            for ( Map.Entry<Threshold, Set<Threshold>> nextThresholds : grouped.entrySet() )
            {
                ThresholdType type = nextThresholds.getKey()
                                                   .type();
                Set<Threshold> thresholdSet = nextThresholds.getValue();
                if ( type == ThresholdType.PROBABILITY )
                {
                    writer.writeFieldName( "probability_thresholds" );
                    THRESHOLDS_SERIALIZER.serialize( thresholdSet, writer, serializers );
                }
                else if ( type == ThresholdType.VALUE )
                {
                    writer.writeFieldName( "value_thresholds" );
                    THRESHOLDS_SERIALIZER.serialize( thresholdSet, writer, serializers );
                }
                else if ( type == ThresholdType.PROBABILITY_CLASSIFIER )
                {
                    writer.writeFieldName( "classifier_thresholds" );
                    THRESHOLDS_SERIALIZER.serialize( thresholdSet, writer, serializers );
                }
            }
            writer.writeEndObject();
        }
    }

    @Override
    public boolean isEmpty( SerializerProvider provider, Set<Threshold> thresholds )
    {
        return thresholds.isEmpty();
    }

    /**
     * Groups the thresholds by their common metadata.
     * @param thresholds the thresholds
     * @return the grouped thresholds
     */

    private Map<Threshold, Set<Threshold>> groupThresholdsByType( Set<Threshold> thresholds )
    {
        Map<Threshold, Set<Threshold>> grouped = new HashMap<>();

        for ( Threshold next : thresholds )
        {
            // Index using a threshold without values/probabilities
            wres.statistics.generated.Threshold nextInner = next.threshold()
                                                                .toBuilder()
                                                                .clearRightThresholdValue()
                                                                .clearRightThresholdProbability()
                                                                .clearLeftThresholdValue()
                                                                .clearLeftThresholdProbability()
                                                                .build();

            Threshold outer = new Threshold( nextInner, next.type(), next.featureName() );

            if ( grouped.containsKey( outer ) )
            {
                grouped.get( outer )
                       .add( next );
            }
            else
            {
                Set<Threshold> nextGroup = new HashSet<>();
                nextGroup.add( next );
                grouped.put( outer, nextGroup );
            }
        }

        return grouped;
    }

}

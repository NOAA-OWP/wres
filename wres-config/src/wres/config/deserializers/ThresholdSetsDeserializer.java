package wres.config.deserializers;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.Threshold;

/**
 * Custom deserializer for an array of threshold sets. Each threshold within each set is deserialized to a
 * {@link Threshold}.
 *
 * @author James Brown
 */
public class ThresholdSetsDeserializer extends ValueDeserializer<Set<Threshold>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdSetsDeserializer.class );

    /** Deserializer for individual threshold sets. */
    private static final ThresholdsDeserializer THRESHOLDS_DESERIALIZER = new ThresholdsDeserializer();

    @Override
    public Set<Threshold> deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext mapper = jp.objectReadContext();
        JsonNode node = mapper.readTree( jp );

        // Preserve insertion order
        Set<Threshold> thresholds = new LinkedHashSet<>();

        int nodeCount = node.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = node.get( i );

            for ( Map.Entry<String, JsonNode> nextThresholds : nextNode.properties() )
            {
                String nextName = nextThresholds.getKey();
                LOGGER.debug( "Discovered a set of thresholds named {}.", nextName );
                JsonNode nextThresholdSet = nextThresholds.getValue();
                Set<Threshold> deserializedThresholds =
                        THRESHOLDS_DESERIALIZER.deserialize( nextThresholdSet, nextName );
                thresholds.addAll( deserializedThresholds );
            }
        }

        return Collections.unmodifiableSet( thresholds );
    }
}


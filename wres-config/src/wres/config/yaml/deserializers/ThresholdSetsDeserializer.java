package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.Threshold;

/**
 * Custom deserializer for an array of threshold sets. Each threshold within each set is deserialized to a
 * {@link Threshold}.
 *
 * @author James Brown
 */
public class ThresholdSetsDeserializer extends JsonDeserializer<Set<Threshold>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdSetsDeserializer.class );

    /** Deserializer for individual threshold sets. */
    private static final ThresholdsDeserializer THRESHOLDS_DESERIALIZER = new ThresholdsDeserializer();

    @Override
    public Set<Threshold> deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        Set<Threshold> thresholds = new HashSet<>();

        int nodeCount = node.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = node.get( i );

            Iterator<Map.Entry<String, JsonNode>> iterator = nextNode.fields();
            while ( iterator.hasNext() )
            {
                Map.Entry<String, JsonNode> nextThresholds = iterator.next();
                String nextName = nextThresholds.getKey();
                LOGGER.debug( "Discovered a set of thresholds named {}.", nextName );
                JsonNode nextThresholdSet = nextThresholds.getValue();
                Set<Threshold> deserializedThresholds =
                        THRESHOLDS_DESERIALIZER.deserialize( mapper, nextThresholdSet, nextName );
                thresholds.addAll( deserializedThresholds );
            }
        }

        return Collections.unmodifiableSet( thresholds );
    }
}


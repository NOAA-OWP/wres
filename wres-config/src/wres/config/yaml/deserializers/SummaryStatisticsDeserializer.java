package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.DurationScoreMetric;

/**
 * Custom deserializer for a {@link DurationScoreMetric.DurationScoreMetricComponent.ComponentName}.
 *
 * @author James Brown
 */
public class SummaryStatisticsDeserializer
        extends JsonDeserializer<Set<DurationScoreMetric.DurationScoreMetricComponent.ComponentName>>
{
    @Override
    public Set<DurationScoreMetric.DurationScoreMetricComponent.ComponentName> deserialize( JsonParser jp,
                                                                                            DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        return StreamSupport.stream( node.spliterator(), false )
                            .map( DeclarationFactory::getEnumName )
                            .map( DurationScoreMetric.DurationScoreMetricComponent.ComponentName::valueOf )
                            // Preserve insertion order
                            .collect( Collectors.toCollection( LinkedHashSet::new ) );
    }
}


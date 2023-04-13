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

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationUtilities;
import wres.statistics.generated.DurationScoreMetric;

/**
 * Custom deserializer for a {@link MetricConstants} that represents a
 * {@link MetricConstants.MetricGroup#UNIVARIATE_STATISTIC}.
 *
 * @author James Brown
 */
public class SummaryStatisticsDeserializer
        extends JsonDeserializer<Set<MetricConstants>>
{
    @Override
    public Set<MetricConstants> deserialize( JsonParser jp,
                                             DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        return StreamSupport.stream( node.spliterator(), false )
                            .map( JsonNode::asText )
                            .map( DeclarationUtilities::toEnumName )
                            // Use the canonical list of summary statistic names as a filter
                            .map( DurationScoreMetric.DurationScoreMetricComponent.ComponentName::valueOf )
                            .map( next -> MetricConstants.valueOf( next.name() ) )
                            // Preserve insertion order
                            .collect( Collectors.toCollection( LinkedHashSet::new ) );
    }
}


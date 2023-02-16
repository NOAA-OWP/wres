package wres.config.yaml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory.Metric;
import wres.config.yaml.DeclarationFactory.MetricParameters;
import wres.config.yaml.DeclarationFactory.Threshold;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.MetricName;

/**
 * Custom deserializer for metrics that are composed of a plain metric name or a name and other attributes.
 *
 * @author James Brown
 */
class MetricsDeserializer extends JsonDeserializer<List<Metric>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MetricsDeserializer.class );

    /** Threshold deserializer. */
    private static final ThresholdsDeserializer THRESHOLDS_DESERIALIZER = new ThresholdsDeserializer();

    /** Probability thresholds string used repeatedly. */
    public static final String PROBABILITY_THRESHOLDS = "probability_thresholds";

    /** Value thresholds string used repeatedly. */
    public static final String VALUE_THRESHOLDS = "value_thresholds";

    /** Classifier thresholds string used repeatedly. */
    public static final String CLASSIFIER_THRESHOLDS = "classifier_thresholds";

    @Override
    public List<Metric> deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        // Metrics with parameters
        if ( node instanceof ObjectNode )
        {
            TreeNode metricsNode = node.get( "metrics" );
            return this.getMetricsFromArray( ( ArrayNode ) metricsNode, reader );
        }
        // Plain array of metrics
        else if ( node instanceof ArrayNode arrayNode )
        {
            return this.getMetricsFromArray( arrayNode, reader );
        }
        else
        {
            throw new IOException( "When reading the '" + jp.currentName()
                                   + "' declaration of 'metrics', discovered an unrecognized data type. Please "
                                   + "fix this declaration and try again." );
        }
    }

    /**
     * Creates a collection of metrics from an array node.
     * @param metricsNode the metrics node
     * @param reader the reader
     * @return the metrics
     * @throws IOException if a metric could not be parsed
     */

    private List<Metric> getMetricsFromArray( ArrayNode metricsNode,
                                              ObjectReader reader ) throws IOException
    {
        List<Metric> metrics = new ArrayList<>();
        int nodeCount = metricsNode.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = metricsNode.get( i );
            Metric nextMetric;

            if ( nextNode.has( "name" ) )
            {
                nextMetric = this.getMetric( nextNode, reader );
                LOGGER.debug( "Discovered a metric with parameters, {}. ", nextMetric.name() );
            }
            else
            {
                String nameString = this.getEnumFriendlyName( nextNode );
                MetricName metricName = MetricName.valueOf( nameString );
                nextMetric = new Metric( metricName, null );
                LOGGER.debug( "Discovered a metric without parameters, {}. ", nextMetric.name() );
            }

            metrics.add( nextMetric );
        }

        return metrics;
    }

    /**
     * Creates a metric from a json node.
     * @param node the metric node
     * @param reader the reader
     * @return the metric
     * @throws IOException if the metric could not be parsed
     */

    private Metric getMetric( JsonNode node, ObjectReader reader ) throws IOException
    {
        MetricParameters parameters = null;
        JsonNode nameNode = node.get( "name" );
        String enumName = this.getEnumFriendlyName( nameNode );
        MetricName metricName = MetricName.valueOf( enumName );
        Set<ComponentName> summaryStatistics = null;
        Set<Threshold> probabilityThresholds = null;
        Set<Threshold> valueThresholds = null;
        Set<Threshold> classifierThresholds = null;
        int minimumSampleSize = 0;
        boolean hasParameters = false;

        // Summary statistics for timing errors
        if ( node.has( "summary_statistics" ) )
        {
            JsonNode summaryStatisticsNode = node.get( "summary_statistics" );
            summaryStatistics = StreamSupport.stream( summaryStatisticsNode.spliterator(), false )
                                             .map( this::getEnumFriendlyName )
                                             .map( ComponentName::valueOf )
                                             .collect( Collectors.toUnmodifiableSet() );
            hasParameters = true;
        }

        // Probability thresholds
        if ( node.has( PROBABILITY_THRESHOLDS ) )
        {
            JsonNode thresholdsNode = node.get( PROBABILITY_THRESHOLDS );
            probabilityThresholds = THRESHOLDS_DESERIALIZER.deserialize( reader,
                                                                         thresholdsNode,
                                                                         true,
                                                                         PROBABILITY_THRESHOLDS );
            hasParameters = true;
        }

        // Value thresholds
        if ( node.has( VALUE_THRESHOLDS ) )
        {
            JsonNode thresholdsNode = node.get( VALUE_THRESHOLDS );
            valueThresholds = THRESHOLDS_DESERIALIZER.deserialize( reader,
                                                                   thresholdsNode,
                                                                   false,
                                                                   VALUE_THRESHOLDS );
            hasParameters = true;
        }

        // Classifier thresholds
        if ( node.has( CLASSIFIER_THRESHOLDS ) )
        {
            JsonNode thresholdsNode = node.get( CLASSIFIER_THRESHOLDS );
            classifierThresholds = THRESHOLDS_DESERIALIZER.deserialize( reader,
                                                                        thresholdsNode,
                                                                        true,
                                                                        CLASSIFIER_THRESHOLDS );
            hasParameters = true;
        }

        // Minimum sample size
        if ( node.has( "minimum_sample_size" ) )
        {
            JsonNode minimumSampleSizeNode = node.get( "minimum_sample_size" );
            minimumSampleSize = minimumSampleSizeNode.asInt();
            hasParameters = true;
        }

        if ( hasParameters )
        {
            parameters = new MetricParameters( probabilityThresholds,
                                               valueThresholds,
                                               classifierThresholds,
                                               summaryStatistics,
                                               minimumSampleSize );
        }

        return new Metric( metricName, parameters );
    }

    /**
     * Returns an enum-friendly name from a node whose text value corresponds to an enum.
     * @param node the enum node
     * @return the enum-friendly name
     */

    private String getEnumFriendlyName( JsonNode node )
    {
        return node.asText().toUpperCase().replace( " ", "_" );
    }
}

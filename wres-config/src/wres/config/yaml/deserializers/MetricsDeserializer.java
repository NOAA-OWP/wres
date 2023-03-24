package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

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

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;

/**
 * Custom deserializer for metrics that are composed of a plain metric name or a name and other attributes.
 *
 * @author James Brown
 */
public class MetricsDeserializer extends JsonDeserializer<Set<Metric>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MetricsDeserializer.class );

    @Override
    public Set<Metric> deserialize( JsonParser jp, DeserializationContext context ) throws IOException
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

    private Set<Metric> getMetricsFromArray( ArrayNode metricsNode,
                                             ObjectReader reader ) throws IOException
    {
        // Preserve insertion order
        Set<Metric> metrics = new LinkedHashSet<>();
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
                String nameString = DeclarationFactory.getFriendlyName( nextNode );
                MetricConstants metricName = MetricConstants.valueOf( nameString );
                nextMetric = new Metric( metricName, null );
                LOGGER.debug( "Discovered a metric without parameters, {}. ", nextMetric.name() );
            }

            metrics.add( nextMetric );
        }

        return Collections.unmodifiableSet( metrics );
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
        JsonNode nameNode = node.get( "name" );
        String enumName = DeclarationFactory.getFriendlyName( nameNode );
        MetricConstants metricName = MetricConstants.valueOf( enumName );
        MetricParameters parameters = this.getMetricParameters( node, reader );
        return new Metric( metricName, parameters );
    }

    /**
     * Reads the metric parameters from a json node, if any.
     * @param node the metric node
     * @param reader the reader
     * @return the metric parameters or null
     * @throws IOException if the metric parameters could not be read
     */

    private MetricParameters getMetricParameters( JsonNode node, ObjectReader reader ) throws IOException
    {
        MetricParameters parameters = null;

        if ( node.size() > 1 )
        {
            parameters = reader.readValue( node, MetricParameters.class );
        }

        return parameters;
    }

}

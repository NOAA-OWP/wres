package wres.config.deserializers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.TreeNode;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
import wres.config.MetricConstants;
import wres.config.DeclarationUtilities;
import wres.config.components.Metric;
import wres.config.components.MetricParameters;

/**
 * Custom deserializer for metrics that are composed of a plain metric name or a name and other attributes.
 *
 * @author James Brown
 */
public class MetricsDeserializer extends ValueDeserializer<Set<Metric>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MetricsDeserializer.class );

    @Override
    public Set<Metric> deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
        JsonNode node = reader.readTree( jp );

        // Metrics with parameters
        if ( node instanceof ObjectNode )
        {
            TreeNode metricsNode = node.get( "metrics" );
            return this.getMetricsFromArray( ( ArrayNode ) metricsNode, mapper );
        }
        // Plain array of metrics
        else if ( node instanceof ArrayNode arrayNode )
        {
            return this.getMetricsFromArray( arrayNode, mapper );
        }
        // Singleton without parameters
        else if ( node instanceof StringNode textNode )
        {
            String nameString = DeclarationUtilities.toEnumName( textNode.asString() );
            MetricConstants metricName = MetricConstants.valueOf( nameString );
            LOGGER.debug( "Discovered a singleton metric without parameters: {}. ", nameString );
            Metric metric = new Metric( metricName, null );
            return Collections.singleton( metric );
        }
        else
        {
            throw new UncheckedIOException( new IOException( "When reading the '" + jp.currentName()
                                                             + "' declaration of 'metrics', discovered an unrecognized "
                                                             + "data type: "
                                                             + node.getClass()
                                                             + ". Please fix this declaration and try again." ) );
        }
    }

    /**
     * Creates a collection of metrics from an array node.
     * @param metricsNode the metrics node
     * @param reader the reader
     * @return the metrics
     */

    private Set<Metric> getMetricsFromArray( ArrayNode metricsNode,
                                             ObjectMapper reader )
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
                LOGGER.debug( "Discovered a metric with parameters: {}. ", nextMetric.name() );
            }
            else
            {
                String nameString = DeclarationUtilities.toEnumName( nextNode.asString() );
                MetricConstants metricName = MetricConstants.valueOf( nameString );
                nextMetric = new Metric( metricName, null );
                LOGGER.debug( "Discovered a metric without parameters: {}. ", nextMetric.name() );
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
     */

    private Metric getMetric( JsonNode node, ObjectMapper reader )
    {
        JsonNode nameNode = node.get( "name" );
        String enumName = DeclarationUtilities.toEnumName( nameNode.asString() );
        MetricConstants metricName = MetricConstants.valueOf( enumName );
        MetricParameters parameters = this.getMetricParameters( node, reader );
        return new Metric( metricName, parameters );
    }

    /**
     * Reads the metric parameters from a json node, if any.
     * @param node the metric node
     * @param mapper the mapper
     * @return the metric parameters or null
     */

    private MetricParameters getMetricParameters( JsonNode node, ObjectMapper mapper )
    {
        MetricParameters parameters = null;

        ObjectReader reader = mapper.readerFor( MetricParameters.class );

        if ( node.size() > 1 )
        {
            parameters = reader.readValue( node );
        }

        return parameters;
    }

}

package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;
import wres.statistics.generated.DurationScoreMetric;

/**
 * Serializes a {@link Metric}.
 * @author James Brown
 */
public class MetricSerializer extends JsonSerializer<Metric>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MetricSerializer.class );

    private static final ThresholdsSerializer THRESHOLDS_SERIALIZER = new ThresholdsSerializer();

    @Override
    public void serialize( Metric metric, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        MetricConstants name = metric.name();
        String friendlyName = DeclarationFactory.getFriendlyName( name.name() );

        // Simple metric
        if ( Objects.isNull( metric.parameters() ) )
        {
            writer.writeString( friendlyName );
        }
        // Metric with parameters
        else
        {
            this.writeMetricWithParameters( metric, friendlyName, writer, serializers );
        }
    }

    /**
     * Writes a metric with parameters.
     * @param metric the metric
     * @param friendlyName the friendly name
     * @param writer the writer
     * @param serializers the serializer provider
     * @throws IOException if the metric could not be written for any reason
     */

    private void writeMetricWithParameters( Metric metric,
                                            String friendlyName,
                                            JsonGenerator writer,
                                            SerializerProvider serializers )
            throws IOException
    {
        LOGGER.debug( "Discovered a metric named {}, which had parameters.", friendlyName );

        MetricParameters parameters = metric.parameters();
        writer.writeStartObject();
        writer.writeStringField( "name", friendlyName );

        // Probability thresholds
        if ( !parameters.probabilityThresholds()
                        .isEmpty() )
        {
            writer.writeFieldName( "probability_thresholds" );
            THRESHOLDS_SERIALIZER.serialize( parameters.probabilityThresholds(), writer, serializers );
        }
        // Value thresholds
        if ( !parameters.valueThresholds()
                        .isEmpty() )
        {
            writer.writeFieldName( "value_thresholds" );
            THRESHOLDS_SERIALIZER.serialize( parameters.valueThresholds(), writer, serializers );
        }
        // Classifier thresholds
        if ( !parameters.classifierThresholds()
                        .isEmpty() )
        {
            writer.writeFieldName( "classifier_thresholds" );
            THRESHOLDS_SERIALIZER.serialize( parameters.classifierThresholds(), writer, serializers );
        }
        // Summary statistics
        if ( !parameters.summaryStatistics()
                        .isEmpty() )
        {
            List<String> mapped = parameters.summaryStatistics().stream()
                                            .map( DurationScoreMetric.DurationScoreMetricComponent.ComponentName::name )
                                            .map( DeclarationFactory::getFriendlyName )
                                            .toList();
            writer.writeObjectField( "summary_statistics", mapped );
        }
        // Minimum sample size, if not default
        if ( parameters.minimumSampleSize() > 0 )
        {
            writer.writeObjectField( "minimum_sample_size", parameters.minimumSampleSize() );
        }
        // PNG graphics, if not default
        if ( Boolean.FALSE.equals( parameters.png() ) )
        {
            writer.writeBooleanField( "png", false );
        }

        // SVG graphics, if not default
        if ( Boolean.FALSE.equals( parameters.svg() ) )
        {
            writer.writeBooleanField( "svg", false );
        }

        writer.writeEndObject();
    }

}

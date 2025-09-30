package wres.config.serializers;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.DeclarationUtilities;
import wres.config.components.Metric;
import wres.config.components.MetricParameters;
import wres.config.components.MetricParametersBuilder;

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
        String friendlyName = DeclarationUtilities.fromEnumName( name.name() );

        // Simple metric
        if ( this.isSimpleMetric( metric.parameters() ) )
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

        // Value thresholds
        if ( !parameters.thresholds()
                        .isEmpty() )
        {
            writer.writeFieldName( "thresholds" );
            THRESHOLDS_SERIALIZER.serialize( parameters.thresholds(), writer, serializers );
        }
        // Probability thresholds
        if ( !parameters.probabilityThresholds()
                        .isEmpty() )
        {
            writer.writeFieldName( "probability_thresholds" );
            THRESHOLDS_SERIALIZER.serialize( parameters.probabilityThresholds(), writer, serializers );
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
            List<String> mapped = parameters.summaryStatistics()
                                            .stream()
                                            .map( n -> n.getStatistic()
                                                        .name() )
                                            .map( DeclarationUtilities::fromEnumName )
                                            .toList();
            writer.writeObjectField( "summary_statistics", mapped );
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

    /**
     * @param parameters the metric parameters
     * @return whether the metric has any non-default parameter values
     */
    private boolean isSimpleMetric( MetricParameters parameters )
    {
        return Objects.isNull( parameters ) || parameters.equals( MetricParametersBuilder.builder()
                                                                                         .build() );
    }

}

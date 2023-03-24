package wres.config.yaml.components;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.SummaryStatisticsDeserializer;
import wres.config.yaml.deserializers.ThresholdsDeserializer;
import wres.config.yaml.serializers.MetricSerializer;
import wres.statistics.generated.DurationScoreMetric;

/**
 * Metric parameters. For simplicity, a single set of metric parameters is abstracted for all metrics. Restrictions on
 * valid parameters for particular metrics are imposed at the schema level.
 *
 * @param probabilityThresholds probability thresholds
 * @param valueThresholds value thresholds
 * @param classifierThresholds probability classifier thresholds
 * @param summaryStatistics summary statistics
 * @param minimumSampleSize the minimum sample size
 */
@RecordBuilder
@JsonIgnoreProperties( ignoreUnknown = true )
public record MetricParameters( @JsonDeserialize( using = ThresholdsDeserializer.class )
                                @JsonProperty( "probability_thresholds" ) Set<Threshold> probabilityThresholds,
                                @JsonDeserialize( using = ThresholdsDeserializer.class )
                                @JsonProperty( "value_thresholds" ) Set<Threshold> valueThresholds,
                                @JsonDeserialize( using = ThresholdsDeserializer.class )
                                @JsonProperty( "classifier_thresholds" ) Set<Threshold> classifierThresholds,
                                @JsonDeserialize( using = SummaryStatisticsDeserializer.class )
                                @JsonProperty( "summary_statistics" ) Set<DurationScoreMetric.DurationScoreMetricComponent.ComponentName> summaryStatistics,
                                @JsonProperty( "minimum_sample_size" ) Integer minimumSampleSize,
                                @JsonProperty( "png" ) Boolean png,
                                @JsonProperty( "svg" ) Boolean svg )
{
    /**
     * Sets the default values.
     * @param probabilityThresholds the probability thresholds
     * @param valueThresholds the value thresholds
     * @param classifierThresholds the probability classifier thresholds
     * @param summaryStatistics the summary statistics
     * @param minimumSampleSize the minimum sample size
     * @param png whether PNG graphics should be created for this metric
     * @param svg whether SVG graphics should be created for this metric
     */
    public MetricParameters
    {
        if ( Objects.isNull( probabilityThresholds ) )
        {
            probabilityThresholds = Collections.emptySet();
        }

        if ( Objects.isNull( valueThresholds ) )
        {
            valueThresholds = Collections.emptySet();
        }

        if ( Objects.isNull( classifierThresholds ) )
        {
            classifierThresholds = Collections.emptySet();
        }

        if ( Objects.isNull( minimumSampleSize ) )
        {
            minimumSampleSize = 0;
        }

        if ( Objects.isNull( summaryStatistics ) )
        {
            summaryStatistics = Collections.emptySet();
        }

        if ( Objects.isNull( png ) )
        {
            png = true;
        }

        if ( Objects.isNull( svg ) )
        {
            svg = true;
        }
    }
}

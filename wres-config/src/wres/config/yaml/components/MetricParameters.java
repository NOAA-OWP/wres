package wres.config.yaml.components;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.SummaryStatisticsDeserializer;
import wres.config.yaml.deserializers.ThresholdsDeserializer;
import wres.statistics.generated.DurationScoreMetric;

/**
 * Metric parameters.
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
                                @JsonProperty( "graphics" ) Boolean graphics )
{
    // Set the default parameter values
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

        if ( Objects.isNull( graphics ) )
        {
            graphics = true;
        }
    }
}
package wres.config.components;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.SummaryStatisticsDeserializer;
import wres.config.deserializers.ThresholdsDeserializer;
import wres.config.serializers.EnsembleAverageTypeSerializer;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;

/**
 * Metric parameters. For simplicity, a single set of metric parameters is abstracted for all metrics. Restrictions on
 * valid parameters for particular metrics are imposed at the schema level.
 *
 * @param probabilityThresholds probability thresholds
 * @param thresholds value thresholds
 * @param classifierThresholds probability classifier thresholds
 * @param summaryStatistics summary statistics
 * @param ensembleAverageType the ensemble average type
 * @param png whether PNG graphics should be created for this metric
 * @param svg whether SVG graphics should be created for this metric
 */
@RecordBuilder
@JsonIgnoreProperties( ignoreUnknown = true )
public record MetricParameters( @JsonDeserialize( using = ThresholdsDeserializer.class )
                                @JsonProperty( "thresholds" ) Set<Threshold> thresholds,
                                @JsonDeserialize( using = ThresholdsDeserializer.class )
                                @JsonProperty( "probability_thresholds" ) Set<Threshold> probabilityThresholds,
                                @JsonDeserialize( using = ThresholdsDeserializer.class )
                                @JsonProperty( "classifier_thresholds" ) Set<Threshold> classifierThresholds,
                                @JsonDeserialize( using = SummaryStatisticsDeserializer.class )
                                @JsonProperty( "summary_statistics" ) Set<SummaryStatistic> summaryStatistics,
                                @JsonSerialize( using = EnsembleAverageTypeSerializer.class )
                                @JsonProperty( "ensemble_average" ) Pool.EnsembleAverageType ensembleAverageType,
                                @JsonProperty( "png" ) Boolean png,
                                @JsonProperty( "svg" ) Boolean svg )
{
    /**
     * Sets the default values.
     * @param thresholds the value thresholds
     * @param probabilityThresholds the probability thresholds
     * @param classifierThresholds the probability classifier thresholds
     * @param summaryStatistics the summary statistics
     * @param ensembleAverageType the ensemble average type
     * @param png whether PNG graphics should be created for this metric
     * @param svg whether SVG graphics should be created for this metric
     */
    public MetricParameters
    {
        if ( Objects.isNull( probabilityThresholds ) )
        {
            probabilityThresholds = Collections.emptySet();
        }
        else
        {
            // Immutable copy, preserving insertion order
            probabilityThresholds = Collections.unmodifiableSet( new LinkedHashSet<>( probabilityThresholds ) );
        }

        if ( Objects.isNull( thresholds ) )
        {
            thresholds = Collections.emptySet();
        }
        else
        {
            // Immutable copy, preserving insertion order
            thresholds = Collections.unmodifiableSet( new LinkedHashSet<>( thresholds ) );
        }

        if ( Objects.isNull( classifierThresholds ) )
        {
            classifierThresholds = Collections.emptySet();
        }
        else
        {
            // Immutable copy, preserving insertion order
            classifierThresholds = Collections.unmodifiableSet( new LinkedHashSet<>( classifierThresholds ) );
        }

        if ( Objects.isNull( summaryStatistics ) )
        {
            summaryStatistics = Collections.emptySet();
        }
        else
        {
            // Immutable copy, preserving insertion order
            summaryStatistics = Collections.unmodifiableSet( new LinkedHashSet<>( summaryStatistics ) );
        }
    }
}

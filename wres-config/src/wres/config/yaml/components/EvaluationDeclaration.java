package wres.config.yaml.components;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.deserializers.CrossPairDeserializer;
import wres.config.yaml.deserializers.DecimalFormatDeserializer;
import wres.config.yaml.deserializers.DurationDeserializer;
import wres.config.yaml.deserializers.MetricsDeserializer;
import wres.config.yaml.deserializers.SummaryStatisticsDeserializer;
import wres.config.yaml.deserializers.ThresholdSetsDeserializer;
import wres.config.yaml.deserializers.ThresholdSourcesDeserializer;
import wres.config.yaml.deserializers.ThresholdsDeserializer;
import wres.config.yaml.serializers.ChronoUnitSerializer;
import wres.config.yaml.serializers.CrossPairSerializer;
import wres.config.yaml.serializers.DecimalFormatSerializer;
import wres.config.yaml.serializers.DurationSerializer;
import wres.config.yaml.serializers.EnsembleAverageTypeSerializer;
import wres.config.yaml.serializers.PositiveIntegerSerializer;
import wres.config.yaml.serializers.ThresholdSetsSerializer;
import wres.config.yaml.serializers.ThresholdsSerializer;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;

/**
 * Root class for an evaluation declaration.
 * @param label an optional label or name for the evaluation
 * @param left the left or observed data sources, required
 * @param right the right or predicted data sources, required
 * @param baseline the baseline data sources
 * @param features the features, optional
 * @param featureGroups the feature groups
 * @param featureService the feature service
 * @param spatialMask a spatial mask
 * @param unit the measurement unit
 * @param unitAliases aliases for measurement units
 * @param referenceDates reference dates
 * @param referenceDatePools reference date pools
 * @param validDates valid dates
 * @param validDatePools valid date pools
 * @param leadTimes lead times
 * @param analysisTimes analysis durations
 * @param leadTimePools lead time pools
 * @param timeScale the evaluation timescale
 * @param rescaleLenience whether rescaling should admit periods with missing values
 * @param pairFrequency the frequency of the paired data
 * @param crossPair whether to conduct cross-pairing of predicted and baseline datasets
 * @param thresholds the value thresholds
 * @param probabilityThresholds the probability thresholds
 * @param classifierThresholds the probability classifier thresholds
 * @param thresholdSets a collection of thresholds sets
 * @param thresholdSources the threshold service
 * @param ensembleAverageType the type of ensemble average to use
 * @param minimumSampleSize the minimum sample size for which metrics should be computed
 * @param sampleUncertainty a request for sampling uncertainty estimation
 * @param season the season
 * @param values the valid values
 * @param metrics the metrics
 * @param summaryStatistics the summary statistics to generate from raw statistics
 * @param durationFormat the duration format
 * @param decimalFormat the decimal format
 * @param formats the statistics formats to write
 */

@RecordBuilder
public record EvaluationDeclaration( @JsonProperty( "label" ) String label,
                                     @JsonProperty( "observed" ) Dataset left,
                                     @JsonProperty( "predicted" ) Dataset right,
                                     @JsonProperty( "baseline" ) BaselineDataset baseline,
                                     @JsonProperty( "features" ) Features features,
                                     @JsonProperty( "feature_groups" ) FeatureGroups featureGroups,
                                     @JsonProperty( "feature_service" ) FeatureService featureService,
                                     @JsonProperty( "spatial_mask" ) SpatialMask spatialMask,
                                     @JsonProperty( "unit" ) String unit,
                                     @JsonProperty( "unit_aliases" ) Set<UnitAlias> unitAliases,
                                     @JsonProperty( "reference_dates" ) TimeInterval referenceDates,
                                     @JsonProperty( "reference_date_pools" ) TimePools referenceDatePools,
                                     @JsonProperty( "valid_dates" ) TimeInterval validDates,
                                     @JsonProperty( "valid_date_pools" ) TimePools validDatePools,
                                     @JsonProperty( "lead_times" ) LeadTimeInterval leadTimes,
                                     @JsonProperty( "lead_time_pools" ) TimePools leadTimePools,
                                     @JsonProperty( "analysis_times" ) AnalysisTimes analysisTimes,
                                     @JsonProperty( "time_scale" ) TimeScale timeScale,
                                     @JsonProperty( "rescale_lenience" ) TimeScaleLenience rescaleLenience,
                                     @JsonSerialize( using = DurationSerializer.class )
                                     @JsonDeserialize( using = DurationDeserializer.class )
                                     @JsonProperty( "pair_frequency" ) Duration pairFrequency,
                                     @JsonSerialize( using = CrossPairSerializer.class )
                                     @JsonDeserialize( using = CrossPairDeserializer.class )
                                     @JsonProperty( "cross_pair" ) CrossPair crossPair,
                                     @JsonSerialize( using = ThresholdsSerializer.class )
                                     @JsonDeserialize( using = ThresholdsDeserializer.class )
                                     @JsonProperty( "thresholds" ) Set<Threshold> thresholds,
                                     @JsonSerialize( using = ThresholdsSerializer.class )
                                     @JsonDeserialize( using = ThresholdsDeserializer.class )
                                     @JsonProperty( "probability_thresholds" ) Set<Threshold> probabilityThresholds,
                                     @JsonSerialize( using = ThresholdsSerializer.class )
                                     @JsonDeserialize( using = ThresholdsDeserializer.class )
                                     @JsonProperty( "classifier_thresholds" ) Set<Threshold> classifierThresholds,
                                     @JsonDeserialize( using = ThresholdSetsDeserializer.class )
                                     @JsonSerialize( using = ThresholdSetsSerializer.class )
                                     @JsonProperty( "threshold_sets" ) Set<Threshold> thresholdSets,
                                     @JsonDeserialize( using = ThresholdSourcesDeserializer.class )
                                     @JsonProperty( "threshold_sources" ) Set<ThresholdSource> thresholdSources,
                                     @JsonSerialize( using = EnsembleAverageTypeSerializer.class )
                                     @JsonProperty( "ensemble_average" ) Pool.EnsembleAverageType ensembleAverageType,
                                     @JsonSerialize( using = PositiveIntegerSerializer.class )
                                     @JsonProperty( "minimum_sample_size" ) Integer minimumSampleSize,
                                     @JsonProperty( "sampling_uncertainty" ) SamplingUncertainty sampleUncertainty,
                                     @JsonProperty( "season" ) Season season,
                                     @JsonProperty( "values" ) Values values,
                                     @JsonDeserialize( using = MetricsDeserializer.class )
                                     @JsonProperty( "metrics" ) Set<Metric> metrics,
                                     @JsonDeserialize( using = SummaryStatisticsDeserializer.class )
                                     @JsonProperty( "summary_statistics" ) Set<SummaryStatistic> summaryStatistics,
                                     @JsonSerialize( using = ChronoUnitSerializer.class )
                                     @JsonProperty( "duration_format" ) ChronoUnit durationFormat,
                                     @JsonSerialize( using = DecimalFormatSerializer.class )
                                     @JsonDeserialize( using = DecimalFormatDeserializer.class )
                                     @JsonProperty( "decimal_format" ) DecimalFormat decimalFormat,
                                     @JsonProperty( "output_formats" ) Formats formats )
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationDeclaration.class );

    /**
     * Sets the default values.
     * @param left the left or observed data sources, required
     * @param right the right or predicted data sources, required
     * @param baseline the baseline data sources
     * @param features the features, optional
     * @param featureGroups the feature groups
     * @param featureService the feature service
     * @param spatialMask a spatial mask
     * @param unit the measurement unit
     * @param unitAliases aliases for measurement units
     * @param referenceDates reference dates
     * @param referenceDatePools reference date pools
     * @param validDates valid dates
     * @param validDatePools valid date pools
     * @param leadTimes lead times
     * @param analysisTimes analysis durations
     * @param leadTimePools lead time pools
     * @param timeScale the evaluation timescale
     * @param rescaleLenience whether rescaling should admit periods with missing values
     * @param pairFrequency the frequency of the paired data
     * @param crossPair whether to conduct cross-pairing of predicted and baseline datasets
     * @param thresholds the value thresholds
     * @param probabilityThresholds the probability thresholds
     * @param classifierThresholds the probability classifier thresholds
     * @param thresholdSets a collection of thresholds sets
     * @param thresholdSources the threshold service
     * @param ensembleAverageType the type of ensemble average to use
     * @param minimumSampleSize the minimum sample size for which metrics should be computed
     * @param sampleUncertainty a request for sampling uncertainty estimation
     * @param season the season
     * @param values the valid values
     * @param metrics the metrics
     * @param summaryStatistics the summary statistics to generate from raw statistics
     * @param durationFormat the duration format
     * @param decimalFormat the decimal format
     * @param formats the statistics formats to write
     * @param label an optional label or name for the evaluation
     */
    public EvaluationDeclaration
    {
        if ( Objects.isNull( metrics ) )
        {
            metrics = this.emptyOrUnmodifiableSet( metrics, "metrics" );
        }

        if ( Objects.isNull( summaryStatistics ) )
        {
            summaryStatistics = this.emptyOrUnmodifiableSet( summaryStatistics, "summary statistics" );
        }

        if ( Objects.isNull( unitAliases ) )
        {
            unitAliases = this.emptyOrUnmodifiableSet( unitAliases, "unit aliases" );
        }

        if ( Objects.isNull( rescaleLenience ) )
        {
            rescaleLenience = TimeScaleLenience.NONE;
        }

        if ( Objects.isNull( probabilityThresholds ) )
        {
            probabilityThresholds = this.emptyOrUnmodifiableSet( probabilityThresholds, "probability thresholds" );
        }

        if ( Objects.isNull( thresholds ) )
        {
            thresholds = this.emptyOrUnmodifiableSet( thresholds, "thresholds" );
        }

        if ( Objects.isNull( classifierThresholds ) )
        {
            classifierThresholds = this.emptyOrUnmodifiableSet( classifierThresholds, "classifier thresholds" );
        }

        if ( Objects.isNull( thresholdSets ) )
        {
            thresholdSets = this.emptyOrUnmodifiableSet( thresholdSets, "threshold sets" );
        }

        if ( Objects.isNull( thresholdSources ) )
        {
            thresholdSources = this.emptyOrUnmodifiableSet( thresholdSources, "threshold sources" );
        }

        if ( Objects.isNull( summaryStatistics ) )
        {
            summaryStatistics = this.emptyOrUnmodifiableSet( summaryStatistics, "summary statistics" );
        }

        if ( Objects.isNull( durationFormat ) )
        {
            durationFormat = ChronoUnit.SECONDS;
        }

        if ( Objects.isNull( minimumSampleSize ) )
        {
            minimumSampleSize = 0;
        }
    }

    /**
     * Returns an unmodifiable set from the possibly null input that preserves insertion order.
     * @param possiblyNull the possibly null set
     * @param origin the origin of the set to help with logging
     * @return an unmodifiable set that preserves insertion order
     * @param <T> the collected data type
     */
    private <T> Set<T> emptyOrUnmodifiableSet( Set<T> possiblyNull, String origin )
    {
        if ( Objects.isNull( possiblyNull ) )
        {
            LOGGER.debug( "Discovered a null set of {}.", origin );
            return Collections.emptySet();
        }
        else
        {
            return Collections.unmodifiableSet( new LinkedHashSet<>( possiblyNull ) );
        }
    }
}

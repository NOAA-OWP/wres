package wres.config.components;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.deserializers.CrossPairDeserializer;
import wres.config.deserializers.DecimalFormatDeserializer;
import wres.config.deserializers.DurationDeserializer;
import wres.config.deserializers.MetricsDeserializer;
import wres.config.deserializers.SummaryStatisticsDeserializer;
import wres.config.deserializers.ThresholdSetsDeserializer;
import wres.config.deserializers.ThresholdSourcesDeserializer;
import wres.config.deserializers.ThresholdsDeserializer;
import wres.config.deserializers.TimeWindowDeserializer;
import wres.config.serializers.ChronoUnitSerializer;
import wres.config.serializers.CrossPairSerializer;
import wres.config.serializers.DecimalFormatSerializer;
import wres.config.serializers.DurationSerializer;
import wres.config.serializers.EnsembleAverageTypeSerializer;
import wres.config.serializers.PositiveIntegerSerializer;
import wres.config.serializers.SummaryStatisticsSerializer;
import wres.config.serializers.ThresholdSetsSerializer;
import wres.config.serializers.ThresholdsSerializer;
import wres.config.serializers.TimeWindowSerializer;
import wres.config.serializers.TrueSerializer;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.TimeWindow;

/**
 * Root class for an evaluation declaration.
 * @param label an optional label or name for the evaluation
 * @param left the left or observed data sources, required
 * @param right the right or predicted data sources, required
 * @param baseline the baseline data sources
 * @param covariates the covariate datasets
 * @param features the features, optional
 * @param featureGroups the feature groups
 * @param featureService the feature service
 * @param spatialMask a spatial mask
 * @param unit the measurement unit
 * @param unitAliases aliases for measurement units
 * @param timePools explicit time pools
 * @param referenceDates reference dates
 * @param referenceDatePools reference date pools
 * @param validDates valid dates
 * @param ignoredValidDates any valid dates whose time-series values should be ignored
 * @param validDatePools valid date pools
 * @param leadTimes lead times
 * @param analysisTimes analysis durations
 * @param leadTimePools lead time pools
 * @param eventDetection event detection
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
 * @param combinedGraphics is true to combine predicted and baseline statistics into the same graphics
 */

@RecordBuilder
public record EvaluationDeclaration( @JsonProperty( "label" ) String label,
                                     @JsonProperty( "observed" ) Dataset left,
                                     @JsonProperty( "predicted" ) Dataset right,
                                     @JsonProperty( "baseline" ) BaselineDataset baseline,
                                     @JsonProperty( "covariates" ) List<CovariateDataset> covariates,
                                     @JsonProperty( "features" ) Features features,
                                     @JsonProperty( "feature_groups" ) FeatureGroups featureGroups,
                                     @JsonProperty( "feature_service" ) FeatureService featureService,
                                     @JsonProperty( "spatial_mask" ) SpatialMask spatialMask,
                                     @JsonProperty( "unit" ) String unit,
                                     @JsonProperty( "unit_aliases" ) Set<UnitAlias> unitAliases,
                                     @JsonSerialize( using = TimeWindowSerializer.class )
                                     @JsonDeserialize( using = TimeWindowDeserializer.class )
                                     @JsonProperty( "time_pools" ) Set<TimeWindow> timePools,
                                     @JsonProperty( "reference_dates" ) TimeInterval referenceDates,
                                     @JsonProperty( "reference_date_pools" ) Set<TimePools> referenceDatePools,
                                     @JsonProperty( "valid_dates" ) TimeInterval validDates,
                                     @JsonProperty( "ignored_valid_dates" ) Set<TimeInterval> ignoredValidDates,
                                     @JsonProperty( "valid_date_pools" ) Set<TimePools> validDatePools,
                                     @JsonProperty( "lead_times" ) LeadTimeInterval leadTimes,
                                     @JsonProperty( "lead_time_pools" ) Set<TimePools> leadTimePools,
                                     @JsonProperty( "event_detection" ) EventDetection eventDetection,
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
                                     @JsonSerialize( using = ThresholdSetsSerializer.class )
                                     @JsonDeserialize( using = ThresholdSetsDeserializer.class )
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
                                     @JsonSerialize( using = SummaryStatisticsSerializer.class )
                                     @JsonDeserialize( using = SummaryStatisticsDeserializer.class )
                                     @JsonProperty( "summary_statistics" ) Set<SummaryStatistic> summaryStatistics,
                                     @JsonSerialize( using = ChronoUnitSerializer.class )
                                     @JsonProperty( "duration_format" ) ChronoUnit durationFormat,
                                     @JsonSerialize( using = DecimalFormatSerializer.class )
                                     @JsonDeserialize( using = DecimalFormatDeserializer.class )
                                     @JsonProperty( "decimal_format" ) DecimalFormat decimalFormat,
                                     @JsonProperty( "output_formats" ) Formats formats,
                                     @JsonSerialize( using = TrueSerializer.class )
                                     @JsonProperty( "combined_graphics" ) Boolean combinedGraphics )
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationDeclaration.class );

    /**
     * Sets the default values.
     * @param left the left or observed data sources, required
     * @param right the right or predicted data sources, required
     * @param baseline the baseline data sources
     * @param covariates the covariate datasets
     * @param features the features, optional
     * @param featureGroups the feature groups
     * @param featureService the feature service
     * @param spatialMask a spatial mask
     * @param unit the measurement unit
     * @param unitAliases aliases for measurement units
     * @param timePools explicit time pools
     * @param referenceDates reference dates
     * @param referenceDatePools reference date pools
     * @param validDates valid dates
     * @param ignoredValidDates any valid dates whose time-series values should be ignored
     * @param validDatePools valid date pools
     * @param leadTimes lead times
     * @param analysisTimes analysis durations
     * @param leadTimePools lead time pools
     * @param eventDetection event detection
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
     * @param combinedGraphics is true to combine predicted and baseline statistics into the same graphics
     */
    public EvaluationDeclaration
    {
        metrics = this.emptyOrUnmodifiableSet( metrics, "metrics" );
        unitAliases = this.emptyOrUnmodifiableSet( unitAliases, "unit aliases" );
        timePools = this.emptyOrUnmodifiableSet( timePools, "time pools" );
        probabilityThresholds = this.emptyOrUnmodifiableSet( probabilityThresholds, "probability thresholds" );
        thresholds = this.emptyOrUnmodifiableSet( thresholds, "thresholds" );
        classifierThresholds = this.emptyOrUnmodifiableSet( classifierThresholds, "classifier thresholds" );
        thresholdSets = this.emptyOrUnmodifiableSet( thresholdSets, "threshold sets" );
        thresholdSources = this.emptyOrUnmodifiableSet( thresholdSources, "threshold sources" );
        summaryStatistics = this.emptyOrUnmodifiableSet( summaryStatistics, "summary statistics" );
        validDatePools = this.emptyOrUnmodifiableSet( validDatePools, "valid date pools" );
        ignoredValidDates = this.emptyOrUnmodifiableSet( ignoredValidDates, "ignored valid dates" );
        referenceDatePools = this.emptyOrUnmodifiableSet( referenceDatePools, "reference date pools" );
        leadTimePools = this.emptyOrUnmodifiableSet( leadTimePools, "lead time pools" );

        if ( Objects.isNull( rescaleLenience ) )
        {
            rescaleLenience = TimeScaleLenience.NONE;
        }

        if ( Objects.isNull( durationFormat ) )
        {
            durationFormat = ChronoUnit.SECONDS;
        }

        if ( Objects.isNull( minimumSampleSize ) )
        {
            minimumSampleSize = 0;
        }

        if ( Objects.isNull( covariates ) )
        {
            covariates = List.of();
        }
        else
        {
            covariates = Collections.unmodifiableList( covariates );
        }

        if ( Objects.isNull( combinedGraphics ) )
        {
            combinedGraphics = false;
        }
    }

    /**
     * @return whether the statistics from the predicted and baseline datasets should be plotted together
     */

    @Override
    public Boolean combinedGraphics()  // NOSONAR
    {
        if ( Objects.isNull( this.combinedGraphics ) )
        {
            return false;
        }

        return this.combinedGraphics;
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

package wres.config.yaml.components;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.BaselineDeserializer;
import wres.config.yaml.deserializers.DatasetDeserializer;
import wres.config.yaml.deserializers.DurationDeserializer;
import wres.config.yaml.deserializers.FeatureGroupDeserializer;
import wres.config.yaml.deserializers.FeaturesDeserializer;
import wres.config.yaml.deserializers.FormatsDeserializer;
import wres.config.yaml.deserializers.MetricsDeserializer;
import wres.config.yaml.deserializers.SeasonDeserializer;
import wres.config.yaml.deserializers.SpatialMaskDeserializer;
import wres.config.yaml.deserializers.ThresholdSetsDeserializer;
import wres.config.yaml.deserializers.ThresholdsDeserializer;
import wres.config.yaml.deserializers.TimeScaleDeserializer;
import wres.statistics.generated.Pool;

/**
 * Root class for an evaluation declaration.
 * @param left the left or observed data sources, required
 * @param right the right or predicted data sources, required
 * @param baseline the baseline data sources, optional
 * @param features the features, optional
 * @param featureService the feature service, optional
 * @param referenceDates the reference times, optional
 * @param referenceDatePools the reference time pools, optional
 * @param validDates the valid times, optional
 * @param validDatePools the valid time pools, optional
 * @param leadTimes the lead times, optional
 * @param leadTimePools the lead time pools, optional
 * @param timeScale the desired timescale, optional
 * @param probabilityThresholds the probability thresholds, optional
 * @param valueThresholds the value thresholds, optional
 * @param classifierThresholds the probability classifier thresholds, optional
 * @param metrics the metrics, optional
 */
@RecordBuilder
public record EvaluationDeclaration( @JsonDeserialize( using = DatasetDeserializer.class )
                                     @JsonProperty( "observed" ) Dataset left,
                                     @JsonDeserialize( using = DatasetDeserializer.class )
                                     @JsonProperty( "predicted" ) Dataset right,
                                     @JsonDeserialize( using = BaselineDeserializer.class )
                                     @JsonProperty( "baseline" ) BaselineDataset baseline,
                                     @JsonDeserialize( using = FeaturesDeserializer.class )
                                     @JsonProperty( "features" ) Features features,
                                     @JsonDeserialize( using = FeatureGroupDeserializer.class )
                                     @JsonProperty( "feature_groups" ) FeatureGroups featureGroups,
                                     @JsonProperty( "feature_service" ) FeatureService featureService,
                                     @JsonDeserialize( using = SpatialMaskDeserializer.class )
                                     @JsonProperty( "spatial_mask" ) SpatialMask spatialMask,
                                     @JsonProperty( "unit" ) String unit,
                                     @JsonProperty( "unit_aliases" ) Set<UnitAlias> unitAliases,
                                     @JsonProperty( "reference_dates" ) TimeInterval referenceDates,
                                     @JsonProperty( "reference_date_pools" ) TimePools referenceDatePools,
                                     @JsonProperty( "valid_dates" ) TimeInterval validDates,
                                     @JsonProperty( "valid_date_pools" ) TimePools validDatePools,
                                     @JsonProperty( "lead_times" ) LeadTimeInterval leadTimes,
                                     @JsonProperty( "analysis_durations" ) AnalysisDurations analysisDurations,
                                     @JsonProperty( "lead_time_pools" ) TimePools leadTimePools,
                                     @JsonDeserialize( using = TimeScaleDeserializer.class )
                                     @JsonProperty( "time_scale" ) TimeScale timeScale,
                                     @JsonProperty( "rescale_lenience" ) TimeScaleLenience rescaleLenience,
                                     @JsonDeserialize( using = DurationDeserializer.class )
                                     @JsonProperty( "pair_frequency" ) Duration pairFrequency,
                                     @JsonProperty( "cross_pair" ) CrossPair crossPair,
                                     @JsonDeserialize( using = ThresholdsDeserializer.class )
                                     @JsonProperty( "probability_thresholds" ) Set<Threshold> probabilityThresholds,
                                     @JsonDeserialize( using = ThresholdsDeserializer.class )
                                     @JsonProperty( "value_thresholds" ) Set<Threshold> valueThresholds,
                                     @JsonDeserialize( using = ThresholdsDeserializer.class )
                                     @JsonProperty( "classifier_thresholds" ) Set<Threshold> classifierThresholds,
                                     @JsonDeserialize( using = ThresholdSetsDeserializer.class )
                                     @JsonProperty( "threshold_sets" ) Set<Threshold> thresholdSets,
                                     @JsonProperty( "ensemble_average" ) Pool.EnsembleAverageType ensembleAverageType,
                                     @JsonDeserialize( using = SeasonDeserializer.class )
                                     @JsonProperty( "season" ) Season season,
                                     @JsonProperty( "values" ) Values values,
                                     @JsonDeserialize( using = MetricsDeserializer.class )
                                     @JsonProperty( "metrics" ) List<Metric> metrics,
                                     @JsonProperty( "duration_format" ) ChronoUnit durationFormat,
                                     @JsonProperty( "decimal_format" ) String decimalFormat,
                                     @JsonDeserialize( using = FormatsDeserializer.class )
                                     @JsonProperty( "output_formats" ) Formats formats )
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationDeclaration.class );

    // Set default values
    public EvaluationDeclaration
    {
        if ( Objects.isNull( metrics ) )
        {
            LOGGER.debug( "No metrics were declared, assuming \"all valid\" metrics are required." );

            metrics = List.of();
        }

        if ( Objects.isNull( rescaleLenience ) )
        {
            rescaleLenience = TimeScaleLenience.NONE;
        }

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

        if ( Objects.isNull( decimalFormat ) )
        {
            decimalFormat = "0.000000";
        }

        if ( Objects.isNull( durationFormat ) )
        {
            durationFormat = ChronoUnit.SECONDS;
        }
    }
}

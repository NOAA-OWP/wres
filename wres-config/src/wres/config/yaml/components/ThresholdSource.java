package wres.config.yaml.components;

import java.net.URI;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.ThresholdSourcesDeserializer;
import wres.config.yaml.deserializers.UriDeserializer;
import wres.config.yaml.serializers.MissingValueSerializer;
import wres.config.yaml.serializers.ThresholdDatasetOrientationSerializer;
import wres.config.yaml.serializers.ThresholdOperatorSerializer;
import wres.config.yaml.serializers.ThresholdOrientationSerializer;
import wres.config.yaml.serializers.ThresholdTypeSerializer;

/**
 * A threshold source.
 * @param uri the URI
 * @param operator the threshold operator
 * @param applyTo the orientation of the dataset to which the thresholds apply
 * @param type the type of thresholds
 * @param parameter the parameter for which thresholds are required
 * @param unit the threshold units
 * @param provider the provider
 * @param ratingProvider the ratings provider
 * @param missingValue the missing value sentinel
 * @param featureNameFrom the dataset whose feature names will be used when requesting thresholds by feature name
 */
@RecordBuilder
public record ThresholdSource( @JsonDeserialize( using = UriDeserializer.class )
                               @JsonProperty( "uri" ) URI uri,
                               @JsonSerialize( using = ThresholdOperatorSerializer.class )
                               @JsonProperty( "operator" ) ThresholdOperator operator,
                               @JsonSerialize( using = ThresholdOrientationSerializer.class )
                               @JsonProperty( "apply_to" ) ThresholdOrientation applyTo,
                               @JsonSerialize( using = ThresholdTypeSerializer.class )
                               @JsonProperty( "type" ) ThresholdType type,
                               @JsonSerialize( using = ThresholdDatasetOrientationSerializer.class )
                               @JsonProperty( "feature_name_from" ) DatasetOrientation featureNameFrom,
                               @JsonProperty( "parameter" ) String parameter,
                               @JsonProperty( "unit" ) String unit,
                               @JsonProperty( "provider" ) String provider,
                               @JsonProperty( "rating_provider" ) String ratingProvider,
                               @JsonSerialize( using = MissingValueSerializer.class )
                               @JsonProperty( "missing_value" ) Double missingValue )
{
    /**
     * Create an instance and set the defaults.
     * @param uri the URI
     * @param operator the threshold operator
     * @param applyTo the orientation of the dataset to which the thresholds apply
     * @param type the type of thresholds
     * @param parameter the parameter for which thresholds are required
     * @param unit the threshold units
     * @param provider the provider
     * @param ratingProvider the ratings provider
     * @param missingValue the missing value sentinel
     * @param featureNameFrom the dataset whose feature names will be used when requesting thresholds by feature name
     */
    public ThresholdSource
    {
        if ( Objects.isNull( operator ) )
        {
            operator = DeclarationFactory.DEFAULT_THRESHOLD_OPERATOR;
        }

        if ( Objects.isNull( applyTo ) )
        {
            applyTo = DeclarationFactory.DEFAULT_THRESHOLD_ORIENTATION;
        }

        if ( Objects.isNull( type ) )
        {
            type = DeclarationFactory.DEFAULT_THRESHOLD_TYPE;
        }

        if ( Objects.isNull( featureNameFrom ) )
        {
            featureNameFrom = DeclarationFactory.DEFAULT_THRESHOLD_DATASET_ORIENTATION;
        }

        if ( Objects.isNull( missingValue ) )
        {
            missingValue = DeclarationFactory.DEFAULT_MISSING_VALUE;
        }
    }
}

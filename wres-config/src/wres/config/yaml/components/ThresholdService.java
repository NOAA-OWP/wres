package wres.config.yaml.components;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.FeatureServiceDeserializer;
import wres.config.yaml.serializers.FeatureServiceSerializer;

/**
 * A threshold service.
 * @param uri the URI
 * @param parameter the parameter for which thresholds are required
 * @param unit the threshold units
 * @param provider the provider
 * @param ratingProvider the ratings provider
 * @param missingValue the missing value sentinel
 * @param featureNameFrom the dataset whose feature names will be used when requesting thresholds by feature name
 */
@RecordBuilder
public record ThresholdService( @JsonProperty( "uri" ) URI uri,
                                @JsonProperty( "parameter" ) String parameter,
                                @JsonProperty( "unit" ) String unit,
                                @JsonProperty( "provider" ) String provider,
                                @JsonProperty( "rating_provider" ) String ratingProvider,
                                @JsonProperty( "missing_value" ) Double missingValue,
                                @JsonProperty( "feature_name_from" ) DatasetOrientation featureNameFrom ) {}

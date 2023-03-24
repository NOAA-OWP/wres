package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.MetricConstants;
import wres.config.yaml.serializers.MetricSerializer;

/**
 * A metric.
 * @param name the metric name
 * @param parameters the metric parameters
 */
@RecordBuilder
@JsonSerialize( using = MetricSerializer.class )
public record Metric( @JsonProperty( "name" ) MetricConstants name, MetricParameters parameters ) {}

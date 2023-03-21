package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.MetricConstants;

/**
 * A metric.
 * @param name the metric name
 * @param parameters the metric parameters
 */
@RecordBuilder
public record Metric( @JsonProperty( "name" ) MetricConstants name, MetricParameters parameters ) {}

package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.statistics.generated.MetricName;

/**
 * A metric.
 * @param name the metric name
 * @param parameters the metric parameters
 */
@RecordBuilder
public record Metric( @JsonProperty( "name" ) MetricName name, MetricParameters parameters ) {}

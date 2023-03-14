package wres.config.yaml.components;

import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * Analysis durations.
 * @param minimum the earliest analysis duration
 * @param maximumExclusive the latest exclusive analysis duration
 */
@RecordBuilder
public record AnalysisDurations( @JsonProperty( "minimum" ) Integer minimum,
                                 @JsonProperty( "maximum_exclusive" ) Integer maximumExclusive,
                                 @JsonProperty( "unit" ) ChronoUnit unit ) {}

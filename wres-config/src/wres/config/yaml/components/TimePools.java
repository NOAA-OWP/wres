package wres.config.yaml.components;

import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * The time pools.
 * @param period the period or duration of each pool
 * @param frequency the spacing between pools
 * @param unit the time unit
 */
@RecordBuilder
public record TimePools( @JsonProperty( "period" ) int period, @JsonProperty( "frequency" ) int frequency,
                         @JsonProperty( "unit" ) ChronoUnit unit ) {}

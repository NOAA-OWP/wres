package wres.config.yaml.components;

import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A lead time interval.
 * @param minimum the earliest time to consider
 * @param maximum the latest time to consider
 * @param unit the time unit
 */
@RecordBuilder
public record LeadTimeInterval( @JsonProperty( "minimum" ) int minimum, @JsonProperty( "maximum" ) int maximum,
                                @JsonProperty( "unit" ) ChronoUnit unit ) {}

package wres.config.components;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A time interval.
 * @param minimum the earliest time to consider
 * @param maximum the latest time to consider
 */
@RecordBuilder
public record TimeInterval( @JsonProperty( "minimum" ) Instant minimum, @JsonProperty( "maximum" ) Instant maximum ) {}

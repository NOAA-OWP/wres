package wres.config.yaml.components;

import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.serializers.ChronoUnitSerializer;

/**
 * A lead time interval.
 * @param minimum the earliest time to consider
 * @param maximum the latest time to consider
 * @param unit the time unit
 */
@RecordBuilder
public record LeadTimeInterval( @JsonProperty( "minimum" ) Integer minimum, @JsonProperty( "maximum" ) Integer maximum,
                                @JsonSerialize( using = ChronoUnitSerializer.class )
                                @JsonProperty( "unit" ) ChronoUnit unit ) {}

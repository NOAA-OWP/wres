package wres.config.yaml.components;

import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.serializers.ChronoUnitSerializer;

/**
 * Analysis durations.
 * @param minimumExclusive the earliest analysis duration, exclusive
 * @param maximum the latest analysis duration
 */
@RecordBuilder
public record AnalysisDurations( @JsonProperty( "minimum_exclusive" ) Integer minimumExclusive,
                                 @JsonProperty( "maximum" ) Integer maximum,
                                 @JsonSerialize( using = ChronoUnitSerializer.class )
                                 @JsonProperty( "unit" ) ChronoUnit unit ) {}

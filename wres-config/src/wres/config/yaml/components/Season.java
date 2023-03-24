package wres.config.yaml.components;

import java.time.MonthDay;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.SeasonDeserializer;
import wres.config.yaml.serializers.SeasonSerializer;

/**
 * A season filter.
 * @param minimum the start of the season
 * @param maximum the end of the season
 */
@RecordBuilder
@JsonSerialize( using = SeasonSerializer.class )
@JsonDeserialize( using = SeasonDeserializer.class )
public record Season( MonthDay minimum, MonthDay maximum ) {}

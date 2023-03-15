package wres.config.yaml.components;

import java.time.MonthDay;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.SeasonDeserializer;

/**
 * A season filter.
 * @param minimum the start of the season
 * @param maximum the end of the season
 */
@RecordBuilder
@JsonDeserialize( using = SeasonDeserializer.class )
public record Season( MonthDay minimum, MonthDay maximum ) {}

package wres.config.yaml.components;

import java.time.MonthDay;

import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A season filter.
 * @param minimum the start of the season
 * @param maximum the end of the season
 */
@RecordBuilder
public record Season( MonthDay minimum, MonthDay maximum ) {}

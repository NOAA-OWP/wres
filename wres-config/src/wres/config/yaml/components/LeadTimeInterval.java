package wres.config.yaml.components;

import java.time.Duration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.LeadTimeIntervalDeserializer;
import wres.config.yaml.serializers.LeadTimeIntervalSerializer;

/**
 * A lead time interval.
 * @param minimum the earliest time to consider
 * @param maximum the latest time to consider
 */
@RecordBuilder
@JsonSerialize( using = LeadTimeIntervalSerializer.class )
@JsonDeserialize( using = LeadTimeIntervalDeserializer.class )
public record LeadTimeInterval( Duration minimum,
                                Duration maximum ) {}

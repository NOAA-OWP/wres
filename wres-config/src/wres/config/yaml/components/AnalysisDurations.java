package wres.config.yaml.components;

import java.time.Duration;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.AnalysisDurationIntervalDeserializer;
import wres.config.yaml.serializers.AnalysisDurationIntervalSerializer;

/**
 * Analysis durations.
 * @param minimumExclusive the earliest analysis duration, exclusive
 * @param maximum the latest analysis duration
 */
@RecordBuilder
@JsonSerialize( using = AnalysisDurationIntervalSerializer.class )
@JsonDeserialize( using = AnalysisDurationIntervalDeserializer.class )
public record AnalysisDurations( Duration minimumExclusive,
                                 Duration maximum ) {}

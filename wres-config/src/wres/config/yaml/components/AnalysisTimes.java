package wres.config.yaml.components;

import java.time.Duration;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.AnalysisDurationIntervalDeserializer;
import wres.config.yaml.serializers.AnalysisDurationIntervalSerializer;

/**
 * Analysis durations.
 * @param minimum the minimum analysis duration
 * @param maximum the maximum analysis duration
 */
@RecordBuilder
@JsonSerialize( using = AnalysisDurationIntervalSerializer.class )
@JsonDeserialize( using = AnalysisDurationIntervalDeserializer.class )
public record AnalysisTimes( Duration minimum,
                             Duration maximum ) {}

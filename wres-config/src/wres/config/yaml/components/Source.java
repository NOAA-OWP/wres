package wres.config.yaml.components;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.TimeScaleDeserializer;

/**
 * A data source.
 * @param uri the URI
 * @param api the API or interface name
 * @param pattern the pattern to consider
 * @param timeZone the time zone
 * @param timeScale the timescale
 * @param missingValue the missing value identifier
 */
@RecordBuilder
public record Source( @JsonProperty( "uri" ) URI uri,
                      @JsonProperty( "interface" ) String api,
                      @JsonProperty( "pattern" ) String pattern,
                      @JsonProperty( "time_zone" ) String timeZone,
                      @JsonDeserialize( using = TimeScaleDeserializer.class ) @JsonProperty( "time_scale" ) TimeScale timeScale,
                      @JsonProperty( "missing_value" ) Double missingValue ) {}

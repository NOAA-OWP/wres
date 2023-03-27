package wres.config.yaml.components;

import java.net.URI;
import java.time.ZoneOffset;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.TimeScaleDeserializer;
import wres.config.yaml.deserializers.ZoneOffsetDeserializer;
import wres.config.yaml.serializers.TimeScaleSerializer;
import wres.config.yaml.serializers.ZoneOffsetSerializer;

/**
 * A data source.
 * @param uri the URI
 * @param api the API or interface name
 * @param pattern the pattern to consider
 * @param timeZoneOffset the time zone
 * @param timeScale the timescale
 * @param missingValue the missing value identifier
 */
@RecordBuilder
public record Source( @JsonProperty( "uri" ) URI uri,
                      @JsonProperty( "interface" ) SourceInterface api,
                      @JsonProperty( "pattern" ) String pattern,
                      @JsonSerialize( using = ZoneOffsetSerializer.class )
                      @JsonDeserialize( using = ZoneOffsetDeserializer.class )
                      @JsonProperty( "time_zone_offset" ) ZoneOffset timeZoneOffset,
                      @JsonSerialize( using = TimeScaleSerializer.class )
                      @JsonDeserialize( using = TimeScaleDeserializer.class )
                      @JsonProperty( "time_scale" ) TimeScale timeScale,
                      @JsonProperty( "missing_value" ) Double missingValue ) {}

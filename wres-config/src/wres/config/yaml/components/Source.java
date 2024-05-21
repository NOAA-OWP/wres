package wres.config.yaml.components;

import java.net.URI;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.UriDeserializer;
import wres.config.yaml.deserializers.ZoneOffsetDeserializer;
import wres.config.yaml.serializers.SourceSerializer;
import wres.config.yaml.serializers.ZoneOffsetSerializer;

/**
 * A data source.
 * @param uri the URI
 * @param sourceInterface the interface name
 * @param parameters the interface parameters
 * @param pattern the pattern to consider
 * @param timeZoneOffset the time zone
 * @param missingValue the missing value identifiers
 * @param unit the measurement unit
 */
@RecordBuilder
@JsonSerialize( using = SourceSerializer.class )
public record Source( @JsonDeserialize( using = UriDeserializer.class )
                      @JsonProperty( "uri" ) URI uri,
                      @JsonProperty( "interface" ) SourceInterface sourceInterface,
                      @JsonProperty( "parameters" ) Map<String, String> parameters,
                      @JsonProperty( "pattern" ) String pattern,
                      @JsonSerialize( using = ZoneOffsetSerializer.class )
                      @JsonDeserialize( using = ZoneOffsetDeserializer.class )
                      @JsonProperty( "time_zone_offset" ) ZoneOffset timeZoneOffset,
                      @JsonProperty( "missing_value" ) List<Double> missingValue,
                      @JsonProperty( "unit" ) String unit )
{
    /**
     * Creates an instance.
     * @param uri the URI
     * @param sourceInterface the interface name
     * @param parameters the interface parameters
     * @param pattern the pattern to consider
     * @param timeZoneOffset the time zone
     * @param missingValue the missing value identifiers
     * @param unit the measurement unit
     */
    public Source
    {
        if ( Objects.isNull( parameters ) )
        {
            parameters = Collections.emptyMap();
        }
        else
        {
            // Immutable copy, preserving insertion order
            parameters = Collections.unmodifiableMap( new LinkedHashMap<>( parameters ) );
        }

        if ( Objects.nonNull( pattern ) && pattern.isBlank() )
        {
            pattern = null;
        }

        if ( Objects.isNull( missingValue ) )
        {
            missingValue = Collections.emptyList();
        }
    }
}

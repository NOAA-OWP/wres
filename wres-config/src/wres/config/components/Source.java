package wres.config.components;

import java.net.URI;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.UriDeserializer;
import wres.config.deserializers.ZoneOffsetDeserializer;
import wres.config.serializers.SourceSerializer;
import wres.config.serializers.ZoneOffsetSerializer;

/**
 * A data source.
 * @param uri the URI
 * @param sourceInterface the interface name
 * @param parameters the interface parameters
 * @param pattern the pattern to consider
 * @param timeZoneOffset the time zone offset, which cannot account for any local rules, such as daylight savings
 * @param timeZone a complete description of the time zone, to be provided rather than an offset, when available
 * @param missingValue the missing value identifiers
 * @param unit the measurement unit
 * @param daylightSavings is false to ignore daylight savings, true to account for it where possible
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
                      TimeZone timeZone,  // Not currently declarable
                      @JsonProperty( "missing_value" ) List<Double> missingValue,
                      @JsonProperty( "unit" ) String unit,
                      @JsonProperty( "daylight_savings" ) Boolean daylightSavings )
{
    /**
     * Creates an instance.
     * @param uri the URI
     * @param sourceInterface the interface name
     * @param parameters the interface parameters
     * @param pattern the pattern to consider
     * @param timeZoneOffset the time zone
     * @param timeZone a complete description of the time zone, to be provided rather than an offset, when available
     * @param missingValue the missing value identifiers
     * @param unit the measurement unit
     * @param daylightSavings is false to ignore daylight savings, true to account for it where possible
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

        if ( Objects.nonNull( pattern )
             && pattern.isBlank() )
        {
            pattern = null;
        }

        if ( Objects.isNull( missingValue ) )
        {
            missingValue = Collections.emptyList();
        }

        if ( Objects.isNull( daylightSavings ) )
        {
            daylightSavings = true;
        }
    }
}

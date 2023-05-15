package wres.config.yaml.components;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.DatasetDeserializer;
import wres.config.yaml.deserializers.TimeScaleDeserializer;
import wres.config.yaml.serializers.DurationSerializer;
import wres.config.yaml.serializers.EnsembleFilterSerializer;
import wres.config.yaml.serializers.TimeScaleSerializer;
import wres.config.yaml.serializers.VariableSerializer;
import wres.config.yaml.serializers.ZoneOffsetSerializer;

/**
 * Observed or predicted dataset.
 * @param label the label
 * @param sources the sources
 * @param variable the variable
 * @param featureAuthority the feature authority
 * @param type the type of data
 * @param ensembleFilter the ensemble filter
 * @param timeShift the time shift
 * @param timeZoneOffset the time zone offset
 * @param timeScale the timescale
 */
@RecordBuilder
@JsonDeserialize( using = DatasetDeserializer.class )
public record Dataset( @JsonProperty( "label" ) String label,
                       @JsonProperty( "sources" ) List<Source> sources,
                       @JsonSerialize( using = VariableSerializer.class )
                       @JsonProperty( "variable" ) Variable variable,
                       @JsonProperty( "feature_authority" ) FeatureAuthority featureAuthority,
                       @JsonProperty( "type" ) DataType type,
                       @JsonSerialize( using = EnsembleFilterSerializer.class )
                       @JsonProperty( "ensemble_filter" ) EnsembleFilter ensembleFilter,
                       @JsonSerialize( using = DurationSerializer.class )
                       @JsonProperty( "time_shift" ) Duration timeShift,
                       @JsonSerialize( using = ZoneOffsetSerializer.class )
                       @JsonProperty( "time_zone_offset" ) ZoneOffset timeZoneOffset,
                       @JsonSerialize( using = TimeScaleSerializer.class )
                       @JsonDeserialize( using = TimeScaleDeserializer.class )
                       @JsonProperty( "time_scale" ) TimeScale timeScale )
{
    /**
     * Set the defaults.
     * @param label the label
     * @param sources the sources
     * @param variable the variable
     * @param featureAuthority the feature authority
     * @param type the type of data
     * @param ensembleFilter the ensemble filter
     * @param timeShift the time shift
     * @param timeZoneOffset the time zone offset
     * @param timeScale the timescale
     */
    public Dataset
    {
        if ( Objects.isNull( sources ) )
        {
            sources = List.of();
        }
        else
        {
            // Immutable
            sources = List.copyOf( sources );
        }
    }
}

package wres.config.components;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.DatasetDeserializer;
import wres.config.deserializers.TimeScaleDeserializer;
import wres.config.serializers.DurationSerializer;
import wres.config.serializers.EnsembleFilterSerializer;
import wres.config.serializers.TimeScaleSerializer;
import wres.config.serializers.VariableSerializer;
import wres.config.serializers.ZoneOffsetSerializer;

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
 * @param unit the measurement unit
 * @param missingValue the missing value identifiers
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
                       @JsonProperty( "time_scale" ) TimeScale timeScale,
                       @JsonProperty( "unit" ) String unit,
                       @JsonProperty( "missing_value" ) List<Double> missingValue )
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
     * @param unit the measurement unit
     * @param missingValue the missing value identifiers
     */
    public Dataset
    {
        if ( Objects.isNull( sources ) )
        {
            sources = Collections.emptyList();
        }
        else
        {
            // Immutable
            sources = List.copyOf( sources );
        }

        if( Objects.isNull( missingValue ) )
        {
            missingValue = Collections.emptyList();
        }
    }
}

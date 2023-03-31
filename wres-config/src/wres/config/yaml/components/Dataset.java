package wres.config.yaml.components;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.DatasetDeserializer;
import wres.config.yaml.serializers.DurationSerializer;
import wres.config.yaml.serializers.EnsembleFilterSerializer;
import wres.config.yaml.serializers.VariableSerializer;

/**
 * Observed or predicted dataset.
 * @param sources the sources
 * @param variable the variable
 * @param featureAuthority the feature authority
 * @param type the type of data
 * @param label the label
 * @param ensembleFilter the ensemble filter
 * @param timeShift the time shift
 */
@RecordBuilder
@JsonDeserialize( using = DatasetDeserializer.class )
public record Dataset( @JsonProperty( "sources" ) List<Source> sources,
                       @JsonSerialize( using = VariableSerializer.class )
                       @JsonProperty( "variable" ) Variable variable,
                       @JsonProperty( "feature_authority" ) FeatureAuthority featureAuthority,
                       @JsonProperty( "type" ) DataType type,
                       @JsonProperty( "label" ) String label,
                       @JsonSerialize( using = EnsembleFilterSerializer.class )
                       @JsonProperty( "ensemble_filter" ) EnsembleFilter ensembleFilter,
                       @JsonSerialize( using = DurationSerializer.class )
                       @JsonProperty( "time_shift" ) Duration timeShift )
{
    /**
     * Set the defaults.
     * @param sources the sources
     * @param variable the variable
     * @param featureAuthority the feature authority
     * @param type the type of data
     * @param label the label
     * @param ensembleFilter the ensemble filter
     * @param timeShift the time shift
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

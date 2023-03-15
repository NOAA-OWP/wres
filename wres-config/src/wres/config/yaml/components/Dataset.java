package wres.config.yaml.components;

import java.time.Duration;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.DatasetDeserializer;

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
                       @JsonProperty( "variable" ) Variable variable,
                       @JsonProperty( "feature_authority" ) String featureAuthority,
                       @JsonProperty( "type" ) DataType type,
                       @JsonProperty( "label" ) String label,
                       @JsonProperty( "ensemble_filter" ) EnsembleFilter ensembleFilter,
                       @JsonProperty( "time_shift" ) Duration timeShift ) {}

package wres.config.yaml.components;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * An ensemble filter.
 * @param members the ensemble members
 * @param exclude whether to exclude the named members, otherwise include them
 */
@RecordBuilder
public record EnsembleFilter( @JsonProperty( "members" ) List<String> members,
                              @JsonProperty( "exclude" ) boolean exclude ) {}

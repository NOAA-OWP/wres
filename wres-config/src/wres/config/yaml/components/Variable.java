package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * The variable to evaluate.
 * @param name the name
 * @param label the label
 */
@RecordBuilder
public record Variable( @JsonProperty( "name" ) String name, @JsonProperty( "label" ) String label ) {}

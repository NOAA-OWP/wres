package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * The baseline data.
 * @param dataset the dataset
 * @param persistence the order of persistence for a persistence baseline
 */
@RecordBuilder
public record BaselineDataset( Dataset dataset,
                               @JsonProperty( "persistence" ) Integer persistence ) {}

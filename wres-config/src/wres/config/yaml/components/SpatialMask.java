package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A spatial mask.
 * @param name the mask name
 * @param wkt a well-known-text string that describes the mask geometry
 */
@RecordBuilder
public record SpatialMask( @JsonProperty( "name" ) String name,
                           @JsonProperty( "wkt" ) String wkt,
                           @JsonProperty( "srid" ) Integer srid ) {}

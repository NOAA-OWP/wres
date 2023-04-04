package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.SpatialMaskDeserializer;

/**
 * A spatial mask.
 * @param name the mask name
 * @param wkt a well-known-text string that describes the mask geometry
 */
@RecordBuilder
@JsonDeserialize( using = SpatialMaskDeserializer.class )
public record SpatialMask( @JsonProperty( "name" ) String name,
                           @JsonProperty( "wkt" ) String wkt,
                           @JsonProperty( "srid" ) Long srid ) {}

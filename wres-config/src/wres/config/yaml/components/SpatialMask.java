package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.locationtech.jts.geom.Geometry;

import wres.config.yaml.deserializers.SpatialMaskDeserializer;
import wres.config.yaml.serializers.SpatialMaskSerializer;

/**
 * A spatial mask.
 * @param name the mask name
 * @param geometry the mask geometry
 */
@RecordBuilder
@JsonSerialize( using = SpatialMaskSerializer.class )
@JsonDeserialize( using = SpatialMaskDeserializer.class )
public record SpatialMask( @JsonProperty( "name" ) String name,
                           Geometry geometry ) {}

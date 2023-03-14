package wres.config.yaml.components;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A feature service.
 * @param uri the URI
 */
@RecordBuilder
public record FeatureService( @JsonProperty( "uri" ) URI uri ) {}

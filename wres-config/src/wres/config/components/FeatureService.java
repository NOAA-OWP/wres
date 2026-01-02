package wres.config.components;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.FeatureServiceDeserializer;
import wres.config.deserializers.UriDeserializer;
import wres.config.serializers.FeatureServiceSerializer;

/**
 * A geospatial feature service.
 * @param uri the URI
 * @param featureGroups the optional feature groups for which to acquire geospatial features
 */
@RecordBuilder
@JsonSerialize( using = FeatureServiceSerializer.class )
@JsonDeserialize( using = FeatureServiceDeserializer.class )
public record FeatureService( @JsonDeserialize( using = UriDeserializer.class )
                              URI uri,
                              Set<FeatureServiceGroup> featureGroups )
{
    /**
     * Sets the default values.
     * @param uri the uri
     * @param featureGroups the feature groups
     */
    public FeatureService
    {
        if ( Objects.isNull( featureGroups ) )
        {
            featureGroups = Collections.emptySet();
        }
        else
        {
            // Immutable copy, preserving insertion order
            featureGroups = Collections.unmodifiableSet( new LinkedHashSet<>( featureGroups ) );
        }
    }
}

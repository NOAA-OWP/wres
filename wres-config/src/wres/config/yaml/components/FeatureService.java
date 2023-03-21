package wres.config.yaml.components;

import java.net.URI;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.FeatureServiceDeserializer;

/**
 * A feature service.
 * @param uri the URI
 */
@RecordBuilder
@JsonDeserialize( using = FeatureServiceDeserializer.class )
public record FeatureService( URI uri,
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
            featureGroups = Set.of();
        }
    }
}

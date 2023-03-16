package wres.config.yaml.components;

import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.FeatureGroupsDeserializer;
import wres.statistics.generated.GeometryGroup;

/**
 * Geographic feature groups.
 * @param geometryGroups the feature groups
 */
@RecordBuilder
@JsonDeserialize( using = FeatureGroupsDeserializer.class )
public record FeatureGroups( Set<GeometryGroup> geometryGroups )
{
    /**
     * Sets the default values.
     * @param geometryGroups the geometry groups
     */
    public FeatureGroups
    {
        if ( Objects.isNull( geometryGroups ) )
        {
            // Undefined is the sentinel for "all valid"
            geometryGroups = Set.of();
        }
    }

    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        StringJoiner joiner = new StringJoiner( ",", "[", "]" );
        for ( GeometryGroup next : geometryGroups )
        {
            String nextString = DeclarationFactory.PROTBUF_STRINGIFIER.apply( next );
            joiner.add( nextString );
        }

        return joiner.toString();
    }
}

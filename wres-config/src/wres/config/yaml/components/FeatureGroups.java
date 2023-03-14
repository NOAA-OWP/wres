package wres.config.yaml.components;

import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.GeometryGroup;

/**
 * Geographic feature groups.
 * @param geometryGroups the feature groups
 */
@RecordBuilder
public record FeatureGroups( Set<GeometryGroup> geometryGroups )
{
    // Set default values
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

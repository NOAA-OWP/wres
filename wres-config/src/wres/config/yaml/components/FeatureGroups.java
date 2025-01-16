package wres.config.yaml.components;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.FeatureGroupsDeserializer;
import wres.config.yaml.serializers.FeatureGroupsSerializer;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Geographic feature groups and associated offset values (e.g., datum offsets). Absence of an offset for a given
 * feature with a group implies no/zero offset.
 * @param geometryGroups the feature groups
 * @param offsets the offset values associated with features in the group, such as a datum offset, if any
 */
@RecordBuilder
@JsonSerialize( using = FeatureGroupsSerializer.class )
@JsonDeserialize( using = FeatureGroupsDeserializer.class )
public record FeatureGroups( Set<GeometryGroup> geometryGroups, Map<GeometryTuple, Offset> offsets )
{
    /**
     * Sets the default values.
     * @param geometryGroups the geometry groups
     * @param offsets the offset values associated with features in the group, such as a datum offset, if any
     */
    public FeatureGroups
    {
        if ( Objects.isNull( geometryGroups ) )
        {
            // Undefined is the sentinel for "all valid"
            geometryGroups = Collections.emptySet();
        }
        else
        {
            // Immutable copy, preserving insertion order
            geometryGroups = Collections.unmodifiableSet( new LinkedHashSet<>( geometryGroups ) );
        }

        if ( Objects.isNull( offsets ) )
        {
            offsets = Collections.emptyMap();
        }
        else
        {
            // Immutable copy, preserving insertion order
            offsets = Collections.unmodifiableMap( new LinkedHashMap<>( offsets ) );
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

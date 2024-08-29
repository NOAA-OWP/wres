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
import wres.config.yaml.deserializers.FeaturesDeserializer;
import wres.config.yaml.serializers.FeaturesSerializer;
import wres.statistics.generated.GeometryTuple;

/**
 * Geographic features and associated offset values (e.g., datum offsets). Absence of an offset for a given feature
 * implies no/zero offset.
 * @param geometries the features
 * @param offsets the offset values, such as a datum offset, if any
 */
@RecordBuilder
@JsonSerialize( using = FeaturesSerializer.class )
@JsonDeserialize( using = FeaturesDeserializer.class )
public record Features( Set<GeometryTuple> geometries, Map<GeometryTuple,Offset> offsets )
{
    /**
     * Sets the default values.
     * @param geometries the geometries
     */
    public Features
    {
        if ( Objects.isNull( geometries ) )
        {
            geometries = Collections.emptySet();
        }
        else
        {
            // Immutable copy, preserving insertion order
            geometries = Collections.unmodifiableSet( new LinkedHashSet<>( geometries ) );
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
        for ( GeometryTuple next : geometries )
        {
            String nextString = DeclarationFactory.PROTBUF_STRINGIFIER.apply( next );
            joiner.add( nextString );
        }

        return joiner.toString();
    }
}

package wres.config.yaml.components;

import java.util.Collections;
import java.util.LinkedHashSet;
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
 * Geographic features.
 * @param geometries the features
 */
@RecordBuilder
@JsonSerialize( using = FeaturesSerializer.class )
@JsonDeserialize( using = FeaturesDeserializer.class )
public record Features( Set<GeometryTuple> geometries )
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

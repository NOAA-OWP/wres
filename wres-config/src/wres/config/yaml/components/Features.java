package wres.config.yaml.components;

import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.GeometryTuple;

/**
 * Geographic features.
 * @param geometries the features
 */
@RecordBuilder
public record Features( Set<GeometryTuple> geometries )
{
    // Set default values
    public Features
    {
        if ( Objects.isNull( geometries ) )
        {
            // Undefined is the sentinel for "all valid"
            geometries = Set.of();
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

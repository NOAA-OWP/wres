package wres.config.yaml.components;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * An ensemble filter.
 * @param members the ensemble members
 * @param exclude whether to exclude the named members, otherwise include them
 */
@RecordBuilder
public record EnsembleFilter( @JsonProperty( "members" ) Set<String> members,
                              @JsonProperty( "exclude" ) boolean exclude )
{
    /**
     * Render the collection of members immutable.
     * @param members the members
     * @param exclude whether to exclude the members
     */
    public EnsembleFilter
    {
        if( Objects.nonNull( members ) )
        {
            // Immutable copy, preserving insertion order
            members = Collections.unmodifiableSet( new LinkedHashSet<>( members ) );
        }
        else
        {
            members = Collections.emptySet();
        }
    }
}

package wres.config.components;

import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * The variable to evaluate.
 * @param name the name
 * @param label the label
 * @param aliases the variable name aliases
 */
@RecordBuilder
public record Variable( @JsonProperty( "name" ) String name,
                        @JsonProperty( "label" ) String label,
                        @JsonProperty( "aliases" ) Set<String> aliases )
{
    /**
     * Set the defaults.
     * @param name the name
     * @param label the label
     * @param aliases the variable name aliases
     */
    public Variable
    {
        if ( Objects.isNull( aliases ) )
        {
            aliases = Set.of();
        }
    }

    /**
     * Returns the preferred name of the variable, specifically the {@link #label()} if available, otherwise the
     * {@link #name()}.
     *
     * @return the preferred name
     */

    @JsonIgnore
    public String getPreferredName()
    {
        if ( Objects.nonNull( this.label() ) )
        {
            return this.label();
        }

        return this.name();
    }
}

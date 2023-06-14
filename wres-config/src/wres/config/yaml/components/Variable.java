package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * The variable to evaluate.
 * @param name the name
 * @param label the label
 */
@RecordBuilder
public record Variable( @JsonProperty( "name" ) String name, @JsonProperty( "label" ) String label )
{
    /**
     * Returns the preferred name of the variable, specifically the {@link #label()} if available, otherwise the
     * {@link #name()}.
     *
     * @return the preferred name
     */

    @JsonIgnore
    public String getPreferredName()
    {
        if( Objects.nonNull( this.label() ) )
        {
            return this.label();
        }

        return this.name();
    }
}

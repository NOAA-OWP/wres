package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A feature service group.
 * @param group the group name
 * @param value the group value
 * @param pool whether the features within the group should be pooled together when computing statistics
 */

@RecordBuilder
@JsonIgnoreProperties( ignoreUnknown = true )
public record FeatureServiceGroup( @JsonProperty( "group" ) String group,
                                   @JsonProperty( "value" ) String value,
                                   @JsonProperty( "pool" ) Boolean pool )
{
    /**
     * Sets the default values.
     * @param group the group
     * @param value the group value
     * @param pool whether the features should be pooled
     */
    public FeatureServiceGroup
    {
        if ( Objects.isNull( pool ) )
        {
            pool = false;
        }
    }
}
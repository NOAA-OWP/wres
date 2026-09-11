package wres.reading.wrds.nwm_legacy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A NWM feature.
 */

@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmFeature
{
    /** The location. */
    private final NwmLocation location;
    /** The members. */
    private final NwmMember[] members;

    /**
     * Creates an instance.
     * @param location the location
     * @param members the members
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmFeature( @JsonProperty( "location" )
                       NwmLocation location,
                       @JsonProperty( "members" )
                       NwmMember[] members )
    {
        this.location = location;
        this.members = members;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "location", location )
                .append( "members", members )
                .toString();
    }
}

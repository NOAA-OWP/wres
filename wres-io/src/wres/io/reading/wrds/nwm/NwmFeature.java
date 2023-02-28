package wres.io.reading.wrds.nwm;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A NWM feature.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmFeature
{
    private final NwmLocation location;
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

    /**
     * @return the location
     */
    public NwmLocation getLocation()
    {
        return this.location;
    }

    /**
     * @return the members
     */
    public NwmMember[] getMembers()
    {
        return this.members;
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

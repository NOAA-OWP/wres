package wres.io.reading.wrds.nwm;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A NWM location.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmLocation
{
    private final NwmLocationNames nwmLocationNames;
    private final NwmMember[] nwmMembers;

    /**
     * Creates an instance.
     * @param nwmLocationNames the NWM location names
     * @param nwmMembers the NWM members
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmLocation( @JsonProperty( "names" )
                        NwmLocationNames nwmLocationNames,
                        @JsonProperty( "members" )
                        NwmMember[] nwmMembers )
    {
        this.nwmLocationNames = nwmLocationNames;
        this.nwmMembers = nwmMembers;
    }

    /**
     * @return the NWM location names
     */
    public NwmLocationNames getNwmLocationNames()
    {
        return this.nwmLocationNames;
    }

    /**
     * @return the NWM members
     */
    public NwmMember[] getNwmMembers()
    {
        return this.nwmMembers;
    }
}
